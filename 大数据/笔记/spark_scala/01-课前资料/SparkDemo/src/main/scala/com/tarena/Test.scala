package com.tarena

/**
 * @author adminstator
 */
object Test {
  

}

class MyClass(str: String) {
  def add() = str + 1
}

class Test(str: String) extends MyClass(str) {
  
}