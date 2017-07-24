package day02.test

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props

class Reduce extends Actor {
  val map = scala.collection.mutable.Map[String,Int]()
  override def receive = {
    case ReduceTask((key,value),rfunc) => {
      println(key + " " + value)
      map += (key) -> rfunc(map.getOrElse(key, 0),value)
       println(map)
    }
  }
  
}

object Reduce {
  def main(args: Array[String]): Unit = {
    val conf = new java.util.HashMap[String, Object]()
    conf.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    val list = new java.util.ArrayList[String]()
    list.add("akka.remote.netty.tcp")
    conf.put("akka.remote.enabled-transports", list)
    conf.put("akka.remote.netty.tcp.hostname", "192.168.221.58")
    conf.put("akka.remote.netty.tcp.port", "2521")
    val system = ActorSystem.create("Reduce", ConfigFactory.parseMap(conf))
    system.actorOf(Props[Reduce],"reduce")
    // akka.tcp://Mapper@192.168.221.58:2521/user/mapper
    
    
  }
}