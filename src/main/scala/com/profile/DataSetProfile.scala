package com.profile

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import com.profile.helper._
import org.apache.spark.sql.types._
import com.profile.udf.Udfs.checkType
import org.apache.spark.sql.Row

//Author  @ gagan SP
object DataSetProfile {


  //Api for checking out the in a columns which are  null
  def checkNullCount(df:Dataset[_])= {
    //collect the column and iterate the columns to check null value
    val columns = df.columns
    df.select(columns.map(coln => sum(df(coln).isNull.cast(IntegerType)).as(coln)): _*)
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

  //transpose existing dataset
  def transpose(df:Dataset[_])={
    val columns=df.columns
    val datset=columns.map(colmn=>df.select(lit(colmn).as("name"),collect_list(df(colmn)).as("value_as_list")).withColumn("value_as_list",col("value_as_list").cast(ArrayType(StringType))))
    val arraySetdatset:Array[Dataset[Row]]=datset.map(ds=>transporseListToCol(ds,df.count))
    arraySetdatset.tail.foldRight(arraySetdatset.head)((itr,acc)=>acc.union(itr))
  }

  /* Below modules to get the columns with respect to thier DataType
  *IntgetType
  * StringType
  * DoubleType
  * LongType
  * BoolenType
  * NumericType

 */


  def getIntCol(df:Dataset[_])={
    val columns=df.schema.filter(struct=> struct.dataType.isInstanceOf[IntegerType]).map(setName=>setName.name)
    df.select(columns.map(colmn=>df(colmn)):_*)
  }

  def getDoubleCol(df:Dataset[_])={
    val columns=df.schema.filter(struct=> struct.dataType.isInstanceOf[DoubleType]).map(setName=>setName.name)
    df.select(columns.map(colmn=>df(colmn)):_*)
  }

  def getLongCol(df:Dataset[_])={
    val columns=df.schema.filter(struct=> struct.dataType.isInstanceOf[LongType]).map(setName=>setName.name)
    df.select(columns.map(colmn=>df(colmn)):_*)
  }

  def getStringCol(df:Dataset[_])={
    val columns=df.schema.filter(struct=> struct.dataType.isInstanceOf[StringType]).map(setName=>setName.name)
    df.select(columns.map(colmn=>df(colmn)):_*)
  }

   def getBoolCol(df:Dataset[_])={
    val columns=df.schema.filter(struct=> struct.dataType.isInstanceOf[BooleanType]).map(setName=>setName.name)
    df.select(columns.map(colmn=>df(colmn)):_*)
  }

  def getNumericCol(df:Dataset[_])={
    val columns=df.schema.filter(struct=> struct.dataType.isInstanceOf[NumericType]).map(setName=>setName.name)
    df.select(columns.map(colmn=>df(colmn)):_*)
  }

}
