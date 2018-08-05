package com.profile.agg

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.profile.udf.Udfs.listToMapUdf

object AggOnDataset {

//Aggregation on Numeric column types
  def aggSummary(df:Dataset[_])={
    val columns=df.schema.filter(struct=>struct.dataType.isInstanceOf[NumericType])
                         .map(struct=>struct.name)
    val DataSetCollection=columns.map(colmns=>{
      df.select(lit(colmns).as("name"),avg(df(colmns)).as("avg"),sum(df(colmns)).as("sum"),count(df(colmns)).as("count"),sum(df(colmns).isNull.cast(IntegerType)).as("count_of_null"),sum(df(colmns).isNotNull.cast(IntegerType)).as("count_of_not_null"),stddev(df(colmns)).as("standardDeviation"),variance(df(colmns)).as("variance"))
    })
    DataSetCollection.tail.foldLeft(DataSetCollection.head)((acc,itr)=>acc.union(itr))
  }


//Module for collecting the column value  as list
  def getColumnValueTOList(df:Dataset[_])={
    val columns=df.columns
    val DataSet=columns.map(colmn=>df.select(lit(colmn).as("name"),collect_list(df(colmn)).as("column_value_list")).withColumn("column_value_list",col("column_value_list").cast(ArrayType(StringType))))

    DataSet.tail.foldLeft(DataSet.head)((acc,itr)=>{ acc.union(itr) })
  }


  // Module to see how many times one values duplicated in the columns
  def groupOnIdententity(df:Dataset[_])={

    var ds=getColumnValueTOList(df)
         ds=ds.withColumn("column_value_map",listToMapUdf(ds("column_value_list"))).drop("column_value_list")
         ds
  }






}
