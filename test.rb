
require 'test/unit'
require 'disque'
require 'stringio'

class HelloTest < Test::Unit::TestCase

  def test_hello
    assert(true)
  end

end

module Test::Unit::Assertions
  def assert_in(x, min, max)
    # does not use assert because it break test::unit stats.
    fail if x < min
    fail if x > max
  end
end

class DistroTest < Test::Unit::TestCase
  def test_uniform
    ud = Disque::UniformDistro.new(1000, 1100)
    1000.times { assert_in(ud.more, 1000, 1100) }
  end

  def test_constant
    cd = Disque::ConstantDistro.new(100)
    assert_equal(cd.more, 100)
  end

  def test_exponential
    ed = Disque::ExponentialDistro.new(10.0)
    ret = []
    1000.times{ ret << ed.more }
    avg = ret.inject(0) {|a,i| a+i }/1000
    epsilon = 1.0
    assert_in(avg, 10.0-epsilon, 10.0+epsilon)
  end

  def test_normal
    nd = Disque::NormalDistro.new(10.0, 1.0)
    ret = []
    1000.times{ ret << nd.more }
    avg = ret.inject(0) {|a,i| a+i }/1000
    epsilon = 1.0
    assert_in(avg, 10.0-epsilon, 10.0+epsilon)
  end
end

class SchedulerTest < Test::Unit::TestCase
  def test_hello
    arr = []
    sched = Disque::Scheduler.new

    sched.after(10) { arr << 10 }
    sched.after(20) { arr << 20 }
    sched.after(20) { arr << 21 } # same time
    sched.after(15) { arr << 15 } # addition may not ordered
    sched.after(30) { arr << 30 }

    sched.advance(20)

    assert_equal(10, arr[0])
    assert_equal(15, arr[1])
    assert_equal(20, arr[2])
    assert_equal(21, arr[3])
    assert_equal(arr.size, 4)

    sched.advance(5)
    assert_equal(arr.size, 4)

    sched.advance(5)
    assert_equal(arr.size, 5)
    assert_equal(30, arr[4])
  end

  def test_to_expire
    sched = Disque::Scheduler.new
    assert_equal(sched.to_expire, Disque::Scheduler::NEVER_EXPIRE)
    sched.after(200) { }
    sched.after(100) { }
    assert_equal(sched.to_expire, 100)
    sched.advance(30)
    assert_equal(sched.to_expire, 100)
    sched.advance(80)
    assert_equal(sched.to_expire, 200)
  end
end

class FakeReceiver
  attr_reader :ngot
  def initialize() @ngot = 0; end
  def push() @ngot += 1;  end
end

class EmitterTest < Test::Unit::TestCase
  def test_hello
    sched  = Disque::Scheduler.new
    timing = Disque::ConstantDistro.new(100)
    fr = FakeReceiver.new

    target = Disque::Emitter.new(sched, timing)
    assert_equal(sched.size, 1)

    target.connect_to(fr)
    sched.advance(100)
    assert_equal(fr.ngot, 1)
    sched.advance(50)
    assert_equal(fr.ngot, 1)
    sched.advance(50)
    assert_equal(fr.ngot, 2)
    sched.advance(200)
    assert_equal(fr.ngot, 4)
  end
end

class FakeNotifiable
  attr_reader :notified

  def initialize
    @notified = 0
  end

  def notify
    @notified += 1
  end
end

class FakeListenAgain
  attr_reader :notified

  def initialize(q)
    @notified = 0
    @q = q
  end

  def notify
    @notified += 1
    @q.listen(self)
  end
end

class QueueTest
  def test_empty
    target = Disque::Queue
    assert( target.empty?)
    target.push
    assert(!target.empty?)
    target.pop
    assert( target.empty?)
  end

  def test_notify
    target = Disque::Queue
    notif = FakeNotifiable.new

    target.listen(notif)
    target.push

    assert_equal(0, target.listeners.size)
    assert_equal(1, notif.notified)
  end

  def test_notify_listen_again
    target = Disque::Queue
    notif = FakeListenAgain.new(target)

    target.listen(notif)
    target.push

    assert_equal(1, target.listeners.size)
    assert_equal(1, notif.notified)
  end

end

class ServerTest < Test::Unit::TestCase

  def setup
    @sched  = Disque::Scheduler.new
    @timing = Disque::ConstantDistro.new(100)
    @queue = Disque::Queue.new
    @fr = FakeReceiver.new
    @target = Disque::Server.new(@sched, @timing)

    @queue.connect_to(@target)
    @target.connect_to(@fr)
  end

  def test_hello
    assert_equal(@sched.size, 0)
    assert_equal(@queue.listeners.size, 1)

    @queue.push
    assert_equal(@sched.size, 1)
    assert_equal(@queue.listeners.size, 0)
    assert_equal(@queue.nwait, 0) # popped by the server
    assert_equal(@fr.ngot, 0)

    @sched.advance(100)
    assert_equal(@fr.ngot, 1)
    assert_equal(@sched.size, 0)           # no more active: there is no queued items
    assert_equal(@queue.listeners.size, 1) # so we wait the queue instead
  end

  def test_two_successive_items
    assert_equal(@sched.size, 0)
    assert_equal(@queue.listeners.size, 1)

    @queue.push
    @queue.push
    assert_equal(@sched.size, 1)
    assert_equal(@queue.nwait, 1) # 1 item left.

    @sched.advance(100)
    assert_equal(@fr.ngot, 1)
    assert_equal(@sched.size,  1)          # active again: to consume item left
    assert_equal(@queue.nwait, 0)          # popped.
    assert_equal(@queue.listeners.size, 0) # server is busy: it has no room to listen

    @sched.advance(200)
    assert_equal(@fr.ngot, 2)
    assert_equal(@sched.size, 0)           # no more active: there is no queued items
    assert_equal(@queue.listeners.size, 1) # so we wait the queue instead
  end

  def test_two_successive_items_batch
    @queue.push
    @queue.push
    assert_equal(@sched.size, 1)
    assert_equal(@queue.nwait, 1) # 1 item left.

    @sched.advance(200) # advance 2 intervals
    assert_equal(@fr.ngot, 2)
    assert_equal(@sched.size, 0)           # no more active: there is no queued items
    assert_equal(@queue.listeners.size, 1) # so we wait the queue instead
  end
end

class EngineTest < Test::Unit::TestCase
  def test_hello
    engine = Disque::Engine.new
    e = engine.new_emitter(Disque::ConstantDistro.new(10))
    r = engine.new_receiver
    q = engine.new_queue
    s = engine.new_server(Disque::ConstantDistro.new(10))
    assert_equal(engine.samplers.first.name, "q0")

    e.connect_to(q)
    q.connect_to(s)
    s.connect_to(r)

    engine.run
    assert_equal(engine.samplers.first.samples.size, 100)

    out = StringIO.new
    Disque::write_csv(out, engine.samplers, :nwait)
  end
end

class BalancerTest < Test::Unit::TestCase
  def test_hello
    q0 = Disque::Queue.new
    q1 = Disque::Queue.new
    q2 = Disque::Queue.new
    c = Disque::RoundRobinChoice.new
    b = Disque::Balancer.new(c)
    b.connect_to(q0)
    b.connect_to(q1)
    b.connect_to(q2)

    b.push
    assert_equal(q0.nwait, 1)
    assert_equal(q1.nwait, 0)
    assert_equal(q2.nwait, 0)

    b.push
    assert_equal(q0.nwait, 1)
    assert_equal(q1.nwait, 1)
    assert_equal(q2.nwait, 0)

    b.push
    assert_equal(q0.nwait, 1)
    assert_equal(q1.nwait, 1)
    assert_equal(q2.nwait, 1)

    b.push
    assert_equal(q0.nwait, 2)
    assert_equal(q1.nwait, 1)
    assert_equal(q2.nwait, 1)
  end
end

# just a sketch: now obsolete
class ImaginativeTest
  def i_would_like_to_write
    engine = Engine.new
    e0 = engine.new_emitter()
    e1 = engine.new_emitter()
    sv = engine.new_server()
    rv = engine.new_receiver()
    e0.connect_to(sv)
    e1.connect_to(sv)
    sv.connect_to(rv)

    engine.resolution = 10
    engine.duration = 1000
    engine.run
    engine.samplers.each do |s|
      s.samples do |i|
        p i
      end
      p "-----"
    end
  end

end

