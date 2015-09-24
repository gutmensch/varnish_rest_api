
require 'open3'
require 'json'
require 'ostruct'
require 'zk'
require 'socket'
require 'varnish_rest_api/version'

class VarnishBase
  
  def initialize(params = {})
    @mgmt_port = params.fetch(:mgmt_port, 6082)
    @port = params.fetch(:port, 4567)
    @mgmt_host = params.fetch(:mgmt_host, 'localhost')
    @instance = params.fetch(:instance, 'default')
    @use_zookeeper = params.fetch(:use_zookeeper, false)
    @zookeeper_host = params.fetch(:zookeeper_host, nil)
    @zookeeper_basenode = params.fetch(:zookeeper_basenode, '/varnish')
    @secret = params.fetch(:secret, '/etc/varnish/secret')
    @varnishadm_path = params.fetch(:varnishadm_path, '/usr/bin/varnishadm')
    @varnishadm = "#{@varnishadm_path.to_s} -T #{@mgmt_host.to_s}:#{@mgmt_port.to_s} -S #{@secret.to_s}"
    @hostname = Socket.gethostname
    @ipaddress = Socket::getaddrinfo(@hostname,"echo",Socket::AF_INET)[0][3]

    puts "varnish_rest_api version #{VarnishRestApiVersion::VERSION}"
    puts "varnishadm command line: " + @varnishadm.to_s

    if @use_zookeeper && !@zookeeper_host.empty?
      puts "configured to use use zookeeper"
      begin
        @zk = ZK.new(@zookeeper_host)
      rescue RuntimeError => e
        abort "problem connecting to zookeeper host: #{@zookeeper_host}"
      end
        
      begin  
        service_node = @zookeeper_basenode + '/service'
        # if the node exists, delete it since it is probably not from our zk session  
        @zk.create(@zookeeper_basenode, :mode => :persistent, :ignore => [:no_node,:node_exists])
        @zk.create(service_node, :mode => :persistent, :ignore => [:no_node,:node_exists])
        # create entries for all backends
        list_backends().each {|b|
          backend_node = service_node + '/' + b.uuid
          varnish_node = backend_node + '/' + @hostname
          @zk.create(backend_node, :mode => :persistent, :ignore => [:no_node,:node_exists])
          @zk.create(varnish_node,"http://#{@ipaddress}:10001/#{b.backend_name}", :mode => :ephemeral, :ignore => [:no_node,:node_exists,:not_empty,:bad_version])
        }
      rescue ZK::Exceptions::NoNode => zke
        $stderr.puts "something went wrong creating the zookeeper node #{node}: " + zke.message
      end
    end
    
  end
  
  def output(result)
    result[:error].empty? ? result[:output] : result[:error]
  end
  
  def varnish_major_version
    varnishadm("banner")[:output].each do |d|
      m = /^varnish-([0-9]+).*/.match(d)
      unless m.nil? 
        return m[1].to_i
      end
    end
    return 0 
  end

  # banning has the effect of purging content
  # https://www.varnish-software.com/static/book/Cache_invalidation.html#banning
  def ban_all 
    command = varnish_major_version >= 4 ?  '\'ban req.url ~ .\'' :  '\'ban.url .\'' 
    result = output(varnishadm(command))
    JSON.pretty_generate({ command => result.empty? ? "command successful" : result })
  end
  
  # ping
  def ping
    JSON.pretty_generate({ 'ping' => output(varnishadm("ping")) })
  end
  
  # backend enable/disable
  def set_health(backend,health,options={})
    default_options = {
     :safe => true,
     :json => false
     }    
    options = default_options.merge!(options)
            
    unless ["sick","auto"].include?(health)
      error = { 'error' => "invalid health '#{health}'. health must be 'sick' or 'auto'"}
      return options[:json] ? JSON.pretty_generate(error) : error 
    end
    
    backends_found = list_backends(:expression => backend)

    if options[:safe] && backends_found.size > 1
      error = { 'error' => "multiple backends found for pattern '#{backend}': " +  backends_found.collect { |b| b.backend_name }.join(',')}
      return  options[:json] ? JSON.pretty_generate(error) : error
    end

    # backend referenced with uuid, get backend title from result
    if backend =~ /[a-fA-F0-9]{40}/
      backend_str = backends_found[0]['backend_name']
    else
      backend_str = backend
    end

    varnishadm("backend.set_health #{backend_str} #{health}")
    list_backends(:expression => backend, :json => options[:json])
  end
   
  # list backends   
  def list_backends(options={})
   default_options = {
    :expression => nil,
    :json => false
    }
    options = default_options.merge!(options)
    backends = Array.new
    command = "backend.list"
    
    unless options[:expression].nil? || options[:expression].empty?
      command += " #{options[:expression]}"
    end
    
    varnishadm_result = filter_backends(command)
    #puts "command => " + command
    
    unless varnishadm_result[:error].empty?
      return options[:json] ? JSON.pretty_generate(varnishadm_result[:error]) : varnishadm_result[:error]
    end
    
    varnishadm_result[:output].to_a.each_with_index do |line,i|
    #varnishadm(command)[:output].to_a.each_with_index do |line,i| 
      next if i < 1
      backend = OpenStruct.new
      #server1(127.0.0.1,80) 1 probe Sick 0/5 
      #line = "server1(127.0.0.1,80) 1 probe Sick 0/5"
      components = line.squeeze.split
      host_re = /(.*?)\((.*?)\)\s+(\d+)\s+(.*?)\s+(.*)/
      match = host_re.match(line)
      backend.backend_name = match[1].to_s
      backend.host = match[2].to_s
      backend.refs = match[3].to_s
      backend.admin = match[4].to_s
      backend.health = match[5].to_s
      backend.uuid = Digest::SHA1.hexdigest "#{backend.host}".gsub(/(,){1,}/,":")
      backend.uuid_str = "#{backend.host}".gsub(/(,){1,}/,":")
      backends << backend
    end
    options[:json] ? JSON.pretty_generate(backends.map { |o| Hash[o.each_pair.to_a] }) : backends
  end

  # wrapper function to also support uuids for services
  def filter_backends(command=command)
    unless command =~ / [a-fA-F0-9]{40}$/
      varnishadm(command)
    else
      output = Array.new
      error = Array.new
      filter_uuid = / ([a-fA-F0-9]{40})$/.match(command)[1]
      varnishadm_result = varnishadm("backend.list")
      unless varnishadm_result[:error].empty?
        return options[:json] ? JSON.pretty_generate(varnishadm_result[:error]) : varnishadm_result[:error]
      end
      varnishadm_result[:output].to_a.each_with_index {|line,i|
        output << line if i < 1
        match = /\((.*)\) /.match(line)
	if match
          host_port = match[1].gsub(/(,){1,}/,':')
          backend_uuid = Digest::SHA1.hexdigest "#{host_port}"
          output << line if backend_uuid == filter_uuid
        end
      }
      return { :output => output , :error => error}
    end
  end

  # Display the varnish banner
  def banner
    JSON.pretty_generate({ 'banner' => output(varnishadm("banner"))})
  end
  
  # Display the current status of the varnish process
  def status
    JSON.pretty_generate({ 'status' => output(varnishadm("status"))}) 
  end
  
  # Run the varnishadm command and capture and return stdout,stderr
  def varnishadm(cmd)  
    output = Array.new 
    error = Array.new  
    begin
      Open3.popen3(@varnishadm + ' ' + cmd) do |stdin, stdout, stderr, wait_thr|        

        exit_status = wait_thr.value
        
        unless exit_status.success?
          #raise
          $stderr.puts "varnishadm exited with code #{exit_status.exitstatus}"
          while line = stderr.gets
            $stderr.puts line
            if line.strip.length > 0
              error << line.strip
            end
          end
        end
        
        while line = stdout.gets
          if line.strip.length > 0          
            output << line.strip
          end
        end        
      end
      
    rescue Errno::ENOENT => e
      $stderr.puts "error running varnishadm: #{e.message}"
      error << "error running varnishadm: #{e.message}"
      output << "error running varnishadm: #{e.message}"
    end
    
    return { :output => output, :error => error}      
  end

  def to_s
    "instance #{@instance}"
  end
  
  private :varnishadm, :varnish_major_version, :output
  
end


=begin
v = Varnish.new
puts v.status
puts "="
puts v.banner
puts "="
puts v.list_backends(:expression => "server3", :json=>true)
puts "="
puts v.list_backends
=end


