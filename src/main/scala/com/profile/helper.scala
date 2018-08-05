package com.profile
import com.sun.prism.PixelFormat.DataType
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object helper {

  val spark=SparkSession.builder().getOrCreate()

  val checkInt=(col:scala.Any)=>{
    try{ if (col.toString.split("/.").size>1) 0; else { col.toString.toInt; 1} } catch { case _:Throwable=>0}
  }

  val checkDouble=(col:scala.Any) =>{
    try{ col.toString.toDouble; 1 } catch { case _:Throwable=>0}
  }

  val checkString=(col:scala.Any)=>{
    try{ col.toString; 1 } catch { case _:Throwable=>0}
  }


  def listToMap(list:mutable.WrappedArray[String])={
  list.groupBy(identity).mapValues(_.size).toSeq.sortBy(_._2)
 }

  //for exploding list structure into single cell value
  def transporseListToCol(df:Dataset[Row],count:Long)={
    val schema=StructType((0 until count.toInt).toArray.map(col=>StructField("col_"+col,StringType,true)))
     var ds=df.rdd.map{row=>
       val element_1:List[Any]=row.getList[String](1).toArray().toList
       val element:List[Any]=row.getString(0)::element_1
       Row.fromSeq(element)
     }
     spark.createDataFrame(ds,schema)
  }


}
