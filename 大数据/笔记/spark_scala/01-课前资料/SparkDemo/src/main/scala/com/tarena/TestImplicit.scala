package com.tarena

import org.json.JSONObject
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
case class Student(name: String, age: Int)
/**
 * @author adminstator
 */
object TestImplicit {
  implicit class StringToColumn(sc: StringContext) {
    def hello(args: Any*): String = {
      "hello," + sc.s(args: _*)

    }
  }

  implicit class StringToJSONObject(sc: StringContext) {
    def j(args: Any*): Map[String,Any] = {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.readValue(sc.s(args: _*), classOf[Map[String,Any]])
    }
  }

  def main(args: Array[String]): Unit = {
    val name = "zhangsan"
    val anothername = "lisi"
    println(s"hello, ${name}, my name is: ${anothername}.")

    val r3 = StringContext("hello, ", ".") // 根据静态部分构造StringContext
      .s(name)
    println(r3)

    val r2 = StringContext("hello, ", ", my name is: ", ".") // 根据静态部分构造StringContext
      .s(name, anothername) // 将动态变量传入s 方法

    println(r2)

    val str2 = hello"ssssssss, $name"
    println(str2)

    
    val map = j"""{"name":"zhangsan", "age":18}"""
    println(map) // 输出 Map(name -> zhangsan, age -> 18)
  }

}

