
require 'disque'

include Disque

#
# compare various load: 0.5, 0.75, 0.9
#

RandomGenerator.clear(0)
engine = Engine.new


e0 = engine.new_emitter(ExponentialDistro.new(20))
r0 = engine.new_receiver
q0 = engine.new_queue
s0 = engine.new_server(NormalDistro.new(10, 5))
e0.connect_to(q0)
q0.connect_to(s0)
s0.connect_to(r0)

e1 = engine.new_emitter(ExponentialDistro.new(20))
r1 = engine.new_receiver
q1 = engine.new_queue
s1 = engine.new_server(NormalDistro.new(15, 5))
e1.connect_to(q1)
q1.connect_to(s1)
s1.connect_to(r1)

e2 = engine.new_emitter(ExponentialDistro.new(20))
r2 = engine.new_receiver
q2 = engine.new_queue
s2 = engine.new_server(NormalDistro.new(18, 5))
e2.connect_to(q2)
q2.connect_to(s2)
s2.connect_to(r2)


engine.resolution =    1*1000
engine.duration   = 5*60*1000
engine.run

Disque::write_csv(STDOUT, engine.samplers, :nwait)
Disque::write_average(STDERR, engine.samplers, :nwait)
