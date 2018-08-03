package com.profile


object helper {

  val checkInt=(col:scala.Any)=>{
    try{ if (col.toString.split("/.").size>1) 0; else { col.toString.toInt; 1} } catch { case _:Throwable=>0}
  }

  val checkDouble=(col:scala.Any) =>{
    try{ col.toString.toDouble; 1 } catch { case _:Throwable=>0}
  }


  val checkString=(col:scala.Any)=>{
    try{ col.toString; 1 } catch { case _:Throwable=>0}
  }

}
