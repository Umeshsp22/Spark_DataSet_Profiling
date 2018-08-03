package com.profile

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.profile.helper._

//Author gagan SP
object DataSetProfile {

  //unified udf for profiling
  def checkType(f:scala.Any=>Int)={
    udf(f)
  }


  //Api for checking out the in a columns which are  null
  def checkNullCount(df:Dataset[_])={
    //collect the column and iterate the columns to check null value
    val columns=df.columns
    df.select(columns.map(coln=>sum(df(coln).isNull.cast(IntegerType)).as(coln)):_*)

  }


  //Api for checking out the in a columns which are not null
  def checkNotNull(df:Dataset[_])={
    val columns=df.columns
    df.select(columns.map(coln=>sum(df(coln).isNotNull.cast(IntegerType)).as(coln)):_*)
  }


  /* module returns the data set based on aggregation on DataType */
  def dtypesCheck(df:Dataset[_])={
    val columns=df.columns
    val dataSet=columns.map(colmn=> {
      var ds=df.select(lit(colmn).as("col_name"),sum(checkType(checkInt)(df(colmn))).as("no_int"),sum(checkType(checkDouble)(df(colmn))).as("no_double"),sum(checkType(checkString)(df(colmn))).as("no_string"))
      ds=ds.withColumn("no_double",abs(ds("no_double")-ds("no_int")))
      ds=ds.withColumn("no_string",abs((ds("no_double")+ds("no_int"))-ds("no_string")))
      ds
    })
    dataSet.tail.foldLeft(dataSet.head)((acc,itr)=>acc.union(itr))
  }

}
