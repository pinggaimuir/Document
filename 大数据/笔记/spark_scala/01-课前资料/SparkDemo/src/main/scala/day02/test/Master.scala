package day02.test
import akka.actor._
import scala.io.Source
import java.util.Scanner
import com.typesafe.config.ConfigFactory

class Master extends Actor {
  override def receive = {
    case MapTask(data,func,rfunc) => {
      val re = func(data)
      println(re)
    }
  }
  
}

object Master {
  def main(args: Array[String]): Unit = {
    val conf = new java.util.HashMap[String, Object]()
    conf.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    val list = new java.util.ArrayList[String]()
    list.add("akka.remote.netty.tcp")
    conf.put("akka.remote.enabled-transports", list)
    conf.put("akka.remote.netty.tcp.hostname", "192.168.221.58")
    conf.put("akka.remote.netty.tcp.port", "2521")
    val system = ActorSystem.create("Mapper", ConfigFactory.parseMap(conf))
    system.actorOf(Props[Master],"mapper")
    // akka.tcp://Mapper@192.168.221.58:2521/user/mapper
  }
}
