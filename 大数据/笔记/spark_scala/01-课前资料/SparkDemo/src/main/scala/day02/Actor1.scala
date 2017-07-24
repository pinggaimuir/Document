package day02

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

case class Student(val name:String) {} // case class 已经实现了序列化接口

class Actor1 extends Actor {
  
  // 用来接收消息
  override def receive = {
    case msg:String => println(msg)
    case i :Int => println(s"接收整型消息$i")
//    case stu:Student => println(s"接收学生消息${stu.name}")
    // Student("zhangsan") // 模式匹配
    case Student(name) => println(s"接收学生消息${name}")
  }
  
  
}

object TestActor1 {
  def main(args: Array[String]): Unit = {
    // 1. 构造Actor的运行环境
    val system = ActorSystem("helloActor")
    // 2. 注册 actor
    val a1ref = system.actorOf(Props[Actor1]) // 会返回一个actor 引用
    // 3. 发送消息
    a1ref ! Student("zhangsan")  // 消息可以是任意支持序列化的对象
   
  }
  
}