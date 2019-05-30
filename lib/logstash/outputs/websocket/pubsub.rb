# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/websocket"

class LogStash::Outputs::WebSocket::Pubsub
  attr_accessor :logger

  def initialize
    @subscribers = []
    @subscribers_lock = Mutex.new
  end # def initialize

  def publish(object)
    @subscribers_lock.synchronize do
      break if @subscribers.size == 0

      failed = []
      @subscribers.each do |subscriber|
        begin
          #@logger.info("Publishing to #{subscriber}, message: #{JSON.parse(object)["message"]}")
          #raise "Failing on purpose" if(JSON.parse(object)["message"] == "fail")
          subscriber.call(object)
        rescue => e
          @logger.error("Failed to publish to subscriber", :subscriber => subscriber, :exception => e)
          failed << subscriber
        end
      end

      failed.each do |subscriber|
        @logger.warn("Dropping the subscriber: ", :subscriber => subscriber)
        @subscribers.delete(subscriber)
      end
    end # @subscribers_lock.synchronize
  end # def Pubsub

  def subscribe(&block)
    #@logger.info("Subscribing a client")
    queue = Queue.new
    @subscribers_lock.synchronize do
      @subscribers << proc do |event|
        #@logger.debug("Adding an event to queue : #{event}")
        queue << event
      end
    end

    while true
      event = queue.pop
      #@logger.info("Processing the queue event #{event}")
      block.call(event)
    end
  end # def subscribe
end # class LogStash::Outputs::WebSocket::Pubsub
