
require 'csv'


# http://ujihisa.nowa.jp/entry/4fa9044239
if RUBY_VERSION < '1.9.0'
  class Array
    def choice
      at(rand(size))
    end
  end
end

module Disque

  #
  # indirection to Kernel.rand (to fake)
  #
  class RandomGenerator

    def rand(max=0) Kernel.rand(max); end

    alias more rand

    @@instance = self.new
    def self.instance() @@instance; end
    def self.clear(seed=0) Kernel.srand(seed); end
  end

  #
  # distribution that always returns same value
  #
  class ConstantDistro
    def initialize(value) @value = value; end
    def more() @value; end
  end

  #
  # http://en.wikipedia.org/wiki/Exponential_distribution
  #
  class ExponentialDistro
    def initialize(mean, ra=RandomGenerator.instance)
      @mean = mean
      @ra = ra
    end

    def more
      x = @ra.more
      (-Math.log(x))*@mean
    end
  end

  #
  # http://en.wikipedia.org/wiki/Normal_distribution
  #
  class NormalDistro
    MIN_VALUE = 0.01

    def initialize(mean, variance, ra=RandomGenerator.instance)
      @mean = mean
      @variance = variance
      @ra = ra
    end

    def more
      u = @ra.more
      v = @ra.more
      x = Math.sqrt(-2.0*Math.log(u))*Math.cos(2.0*Math::PI*v)
      [MIN_VALUE, x*@variance + @mean].max
    end
  end

  #
  # http://en.wikipedia.org/wiki/Uniform_distribution_(continuous)
  #
  class UniformDistro
    def initialize(min, max, ra=RandomGenerator.instance)
      @ra   = ra
      @min  = min
      @max  = max
    end

    def more() @min + @ra.rand(@max - @min); end
  end

  #
  # invoke registered procs until the time comes
  #
  class Scheduler
    class Item < Struct.new(:at, :what); end

    NEVER_EXPIRE = 2**30 # large enough

    attr_reader :items # to debug

    def initialize
      @last    = 0
      @ongoing = nil
      @items = []
    end

    def advance(dur)
      to = @last + dur
      until @items.empty? || to < @items.last.at
        @ongoing = @items.last.at
        i = @items.last
        i.what.call()
        @items.delete(i)
      end
      @ongoing = nil # ensure finish transaction
      @last = to
    end

    def advance_to(to)
      raise unless time < to
      advance(to - time)
    end

    def add(item)
      raise unless time < item.at
      @items << item
      @items.sort! { |a,b| b.at <=> a.at } # NOTE: reversed, and can be lazy.
    end

    def to_expire
      if @items.empty?
        NEVER_EXPIRE
      else
        @items.min{ |a,b| a.at <=> b.at }.at
      end
    end

    def after(dur, &what) add(Item.new(time+dur, what)); end
    def time() @ongoing || @last; end
    def size() @items.size; end
  end

  module Connectable
    def connect_to(dest) @dest = dest; end
  end

  #
  # emit events according the given distribution
  # event destination can be specified with connect_to()
  #
  class Emitter
    include Connectable

    def initialize(sched, timing)
      @sched  = sched
      @timing = timing
      self.schedule
    end

    def emit
      @dest.push if @dest
      self.schedule
    end

    def schedule
      @sched.after(@timing.more) { self.emit }
    end
  end

  #
  # receive outgoing events and just dispose it.
  #
  class Receiver
    def push(); end # do nothing
  end

  #
  # queued pushed items, and notify listeners when pushed
  # destinations should be Server instances
  #
  class Queue
    attr_reader :nwait, :narrivals, :listeners

    def initialize
      @nwait = 0
      @narrivals = 0
      @listeners = Array.new
    end

    def push
      @narrivals += 1
      @nwait += 1
      notify
    end

    def pop
      raise if empty?
      @nwait -= 1
    end

    def listen(l)
      @listeners << l
    end

    def notify
      tonotify = @listeners
      @listeners = Array.new
      tonotify.each { |l| l.notify }
    end

    def empty?() 0 == @nwait; end
    def connect_to(srv) srv.connect_from(self); end # just a sugar
  end

  #
  # consume items from the queue in certain rate
  #
  class Server
    include Connectable

    def initialize(sched, timing)
      @sched  = sched
      @timing = timing
    end

    def connect_from(src)
      raise unless src.empty?
      @src = src
      @src.listen(self)
    end

    def pull
      unless @src.empty?
        @src.pop
        @sched.after(@timing.more) do
          @dest.push
          pull
        end
      else
        @src.listen(self)
      end
    end

    def notify() pull; end # for Queue
  end

  #
  # watch the queue and take snapshots of its stat
  #
  class Sampler
    class Item < Struct.new(:time, :nwait, :arate); end
    attr_reader :name, :target, :samples

    def initialize(name, sched, target)
      @name    = name
      @sched   = sched
      @target  = target
      @samples = Array.new
      @last_narrivals = 0
    end

    def sample
      arrival_rate = @target.narrivals - @last_narrivals
      @last_narrivals = @target.narrivals
      @samples << Item.new(@sched.time, @target.nwait, arrival_rate)
    end
  end

  class Sampler::Item
    # http://en.wikipedia.org/wiki/Little%27s_law
    def staying
      (self.nwait)/(self.arate)
    end
  end

  #
  # dispatch incoming items to one of destinations
  #
  class Balancer
    def initialize(choice)
      @dests = Array.new
      @choice = choice
    end

    def push
      raise if @dests.empty?
      @choice.choose(@dests).push
    end

    def connect_to(d)
      @dests << d
    end
  end

  class RandomChoice
    def choose(dests) dests.choice; end
  end

  class RoundRobinChoice
    def initialize
      @index = -1
    end

    def choose(dests)
      @index = (@index + 1)%dests.size
      dests.at(@index)
    end
  end

  #
  # the simulator. create actors and kick the simulation
  #
  class Engine
    attr_reader :samplers
    attr_accessor :duration, :resolution

    def initialize
      @sched    = Scheduler.new
      @samplers = Array.new
      @duration = 1000
      @resolution = 10
      @time = 0
    end

    def new_emitter(timing) Emitter.new(@sched, timing); end
    def new_receiver() Receiver.new; end
    def new_balancer(choice) Balancer.new(choice); end
    def new_server(timing) Server.new(@sched, timing); end

    def new_queue(name=make_queue_name)
      @samplers << Sampler.new(name, @sched, Queue.new)
      @samplers.last.target
    end

    def run
      raise unless 0 == @time # engine can run only once.
      while @time < @duration
        @time += @resolution
        sample
      end
    end

    def sample
      @sched.advance_to(@time)
      @samplers.each {|s| s.sample }
    end

    private

    def make_queue_name
      "q#{@samplers.size}"
    end
  end

  #
  # utilities
  #

  def self.write_csv(out, samplers, field)
    CSV::Writer.generate(out) do |csv|
      csv << ["time"] + samplers.map{ |s| s.name }
      times  = samplers[0].samples.map{ |s| s.time }
      values = samplers.map{ |s| s.samples.map{ |t| t.send(field) } }
      times.zip(*values).each do |i|
        csv << i
      end
    end
  end

  def self.write_average(out, samplers, field)
    samplers.each do |s|
      avg = s.samples.map{ |x| x.send(field) }.inject(0) {|a,i| a+i }/(s.samples.size.to_f)
      out.print "#{s.name}:#{avg}\n"
    end
  end

end
