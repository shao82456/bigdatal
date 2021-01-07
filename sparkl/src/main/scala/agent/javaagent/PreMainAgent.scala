package agent.javaagent

import java.lang.instrument.Instrumentation

object PreMainAgent {
  def premain(agentArgs: String, inst: Instrumentation) {
    println("premain invoked")
    inst.addTransformer(new InjectJedisAgent())
    inst.addTransformer(new InjectJedisPipeLineAgent())
  }
}
