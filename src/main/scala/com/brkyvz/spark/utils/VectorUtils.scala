package com.brkyvz.spark.utils

object VectorUtils {
  
  def dot(a: Array[Float], b: Array[Float]): Float = {
    var sum = 0f
    val len = a.length
    var i = 0
    while (i < len) {
      sum += a(i) * b(i)
      i += 1
    }
    sum
  }
  
  def sumInto(into: Array[Float], x: Array[Float]): Unit = {
    require(into.length == x.length, "Trying to add vectors with different lengths.")
    val len = into.length
    var i = 0
    while (i < len) {
      into(i) += x(i)
      i += 1
    }
  }

}
