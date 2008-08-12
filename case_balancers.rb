
require 'disque'

include Disque


def connect_to_balancer(engine, b, nemitters, prefix)
  nemitters.times do |i|
    e = engine.new_emitter(ExponentialDistro.new(100))
    q = engine.new_queue("#{prefix}#{i}")
    #s = engine.new_server(NormalDistro.new(100, 50))
    s = engine.new_server(ExponentialDistro.new(100))
    r = engine.new_receiver
    e.connect_to(b)
    b.connect_to(q)
    q.connect_to(s)
    s.connect_to(r)
  end
end

#
# here load is 10/10 = 1. such close-to-full traffic may overflow.
#

RandomGenerator.clear(0)
engine = Engine.new

round = engine.new_balancer(RoundRobinChoice.new)
connect_to_balancer(engine, round, 4, "round")
random = engine.new_balancer(RandomChoice.new)
connect_to_balancer(engine, random, 4, "random")

engine.resolution =     1*1000
engine.duration   = 10*60*1000
engine.run

Disque::write_csv(STDOUT, engine.samplers, :staying)
