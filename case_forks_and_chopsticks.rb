
require 'disque'

include Disque

#
# compare fork (shared queue) model and chopstick (individal queue) model
#

RandomGenerator.clear(0)
engine = Engine.new

nemitters = 4

# fork

fq = engine.new_queue("fork")
fr = engine.new_receiver
nemitters.times do |i|
  fe = engine.new_emitter(ExponentialDistro.new(100))
  #fs = engine.new_server(ExponentialDistro.new(100))
  fs = engine.new_server(NormalDistro.new(100, 50))
  fe.connect_to(fq)
  fq.connect_to(fs)
  fs.connect_to(fr)
end

# chopsticks
cr = engine.new_receiver
nemitters.times do |i|
  ce = engine.new_emitter(ExponentialDistro.new(100))
  #cs = engine.new_server(ExponentialDistro.new(100))
  cs = engine.new_server(NormalDistro.new(100, 50))
  cq = engine.new_queue("cs#{i}")
  ce.connect_to(cq)
  cq.connect_to(cs)
  cs.connect_to(cr)
end

engine.resolution =    1*1000
engine.duration   = 5*60*1000
engine.run

Disque::write_csv(STDOUT, engine.samplers, :staying)
Disque::write_average(STDERR, engine.samplers, :staying)
