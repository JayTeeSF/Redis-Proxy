# frozen_string_literal: true

require 'socket'
require 'yaml'
require 'logger'
require 'time'

# Constants for strings to avoid repeated allocations
MODULE_NAME = 'RedisProxy'.freeze
LISTENING_ON_PORT = 'Listening on port'.freeze
SHUTTING_DOWN = 'Shutting down proxy'.freeze
CONNECTED_PRIMARY = 'Connected to primary at'.freeze
CONNECTED_REPLICA = 'Connected to replica at'.freeze
ERROR_CONNECTING_PRIMARY = 'Error connecting to primary'.freeze
ERROR_CONNECTING_REPLICA = 'Error connecting to replica'.freeze
FORWARDED_DATA = 'Forwarded data from'.freeze
PRIMARY_NODE_HEALTH = 'Primary node health'.freeze
REPLICA_NODE_HEALTH = 'Replica node health'.freeze
DISCONNECTED_FROM = 'Disconnected from'.freeze

# Configuration class
class Config
  attr_accessor :mode, :primary, :replicas, :read_strategy, :health_check_interval, :port, :debug, :replica_timeout

  def self.load(file_path)
    config_data = YAML.load_file(file_path)
    config = Config.new
    config.mode = config_data['mode'].freeze
    config.primary = config_data['primary'].freeze
    config.replicas = config_data['replicas'].map(&:freeze) || []
    config.read_strategy = config_data['read_strategy']&.freeze
    config.health_check_interval = (config_data['health_check_interval'] || 10).freeze
    config.port = (config_data['port'] || '6381').freeze
    config.debug = (config_data['debug'] || false).freeze
    config.replica_timeout = (config_data['replica_timeout'] || 2).freeze
    config
  end
end

# Logging utility with block-based logging
class Logging
  def initialize(debug)
    @logger = Logger.new(STDOUT)
    @logger.level = debug ? Logger::DEBUG : Logger::INFO
  end

  def info(&block)
    @logger.info(&block)
  end

  def debug(&block)
    @logger.debug(&block)
  end

  def error(&block)
    @logger.error(&block)
  end
end

# HealthChecker class
class HealthChecker
  def initialize(config, logger)
    @config = config
    @logger = logger
    @healthy_nodes = {}
  end

  def start
    Thread.new do
      loop do
        check_health(@config.primary)
        @config.replicas.each { |replica| check_health(replica) }
        sleep @config.health_check_interval
      end
    end
  end

  def check_health(node)
    is_healthy = ping_node(node)
    @healthy_nodes[node] = is_healthy
    @logger.debug { "#{node} health: #{is_healthy}" }
  end

  def ping_node(address)
    begin
      socket = Socket.new(:INET, :STREAM)
      sockaddr = Socket.sockaddr_in(@config.replica_timeout, address.split(':').last)
      socket.connect_nonblock(sockaddr)
      socket.close
      true
    rescue Errno::EINPROGRESS, Errno::ECONNREFUSED, Errno::ETIMEDOUT
      false
    end
  end

  def healthy_nodes
    @healthy_nodes.select { |_node, is_healthy| is_healthy }.keys
  end
end

# ConnectionManager class
class ConnectionManager
  def initialize(logger)
    @connections = []
    @logger = logger
  end

  def add_connection(conn)
    @connections << conn
  end

  def get_next_connection
    @connections.shift
  end

  def connect_primary(address)
    connect(address, "primary")
  end

  def connect_replicas(addresses)
    addresses.map { |address| connect(address, "replica") }.compact
  end

  def disconnect(conn)
    conn.close if conn && !conn.closed?
  end

  private

  def connect(address, label)
    begin
      conn = TCPSocket.new(*address.split(':'))
      @logger.debug { "#{CONNECTED_PRIMARY} #{address}" } if label == "primary"
      @logger.debug { "#{CONNECTED_REPLICA} #{address}" } if label == "replica"
      conn
    rescue StandardError => e
      @logger.error { "#{label == "primary" ? ERROR_CONNECTING_PRIMARY : ERROR_CONNECTING_REPLICA} #{address}: #{e.message}" }
      nil
    end
  end
end

# Forwarding class
class Forwarding
  def self.forward_traffic(client_conn, primary_conn, replica_conns, logger)
    data = client_conn.recv(4096)
    primary_conn.send(data, 0)
    replica_conns.each { |replica_conn| replica_conn.send(data, 0) }

    while response = primary_conn.recv(4096)
      client_conn.send(response, 0)
      replica_conns.each { |replica_conn| replica_conn.recv(4096) rescue nil } # We only care about primary response
    end
  rescue StandardError => e
    logger.error { "Error forwarding traffic: #{e.message}" }
  ensure
    [client_conn, primary_conn, *replica_conns].each { |conn| conn.close unless conn.closed? }
  end
end

# Main Proxy class
class RedisProxy
  def initialize(config_file)
    @config = Config.load(config_file)
    @logger = Logging.new(@config.debug)
    @health_checker = HealthChecker.new(@config, @logger)
    @connection_manager = ConnectionManager.new(@logger)
  end

  def start
    @health_checker.start
    server = TCPServer.new('0.0.0.0', @config.port)
    @logger.info { "#{LISTENING_ON_PORT} #{@config.port}" }

    loop do
      client_conn = server.accept
      @connection_manager.add_connection(client_conn)
      handle_connection
    end
  rescue Interrupt
    @logger.info { SHUTTING_DOWN }
  ensure
    server.close if server
  end

  private

  def handle_connection
    Thread.new do
      conn = @connection_manager.get_next_connection
      primary_conn = @connection_manager.connect_primary(@config.primary)
      replica_conns = @connection_manager.connect_replicas(@config.replicas)

      Forwarding.forward_traffic(conn, primary_conn, replica_conns, @logger) if primary_conn
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  config_file = ARGV[0] || 'config.yaml'

  unless File.exist?(config_file)
    puts "Error: Configuration file #{config_file} not found."
    exit 1
  end

  proxy = RedisProxy.new(config_file)
  proxy.start
end
