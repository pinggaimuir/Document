package day02.test

import java.util.Scanner
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory

class Drivers extends Actor{
  override def receive = {
    case a: Task => println(a)
  }
}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new java.util.HashMap[String, Object]()
    conf.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    val list = new java.util.ArrayList[String]()
    list.add("akka.remote.netty.tcp")
    conf.put("akka.remote.enabled-transports", list)
    conf.put("akka.remote.netty.tcp.hostname", "192.168.221.58")
    conf.put("akka.remote.netty.tcp.port", "2522")
    val system = ActorSystem.create("Driver", ConfigFactory.parseMap(conf))
    val sc = new Scanner(System.in);
    val master = system.actorSelection("akka.tcp://Mapper@192.168.221.58:2521/user/mapper")
    val reducer = system.actorSelection("akka.tcp://Reduce@192.168.221.58:2521/user/reduce")
    while(sc.hasNext()){
//      System.out.println(sc.nextLine());
      val line = sc.nextLine();
      val init = 1
//      val task = MapTask(line, (x: String) => x.split(" ").toList.map(x => (x, init)), (a,b)=>(a+b) )
//      val task = 
//      master ! task
      reducer ! ReduceTask((line,1),(a,b)=>a+b)
    }
  }
}