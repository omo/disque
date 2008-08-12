
require 'disque'

include Disque

#
# here load is 10/10 = 1. such close-to-full traffic may overflow.
#

RandomGenerator.clear(0)
engine = Engine.new
e = engine.new_emitter(ExponentialDistro.new(100))
r = engine.new_receiver
q = engine.new_queue
#s = engine.new_server(ExponentialDistro.new(100))
#s = engine.new_server(NormalDistro.new(100, 50))
s = engine.new_server(NormalDistro.new(80, 50))

e.connect_to(q)
q.connect_to(s)
s.connect_to(r)

engine.resolution =    1*1000
engine.duration   = 5*60*1000
engine.run

Disque::write_csv(STDOUT, engine.samplers, :nwait)
