package com.profile.udf

import com.profile.helper.listToMap
import org.apache.spark.sql.functions.udf
import scala.collection.mutable


object Udfs {

  //unified udf for profiling
  def checkType(f:scala.Any=>Int)= {
     udf(f)
  }

  def listToMapUdf={
    udf((list:mutable.WrappedArray[String])=>{
      listToMap(list)
    })
  }
}
