package qadahtm

import Array._

object Grid {
  def main(args: Array[String]) : Unit = {
    val (lat_min,lat_max) = (-90.0,90.0)
    val (lng_min,lng_max) = (-180.0,180.0)
    
    val lat_part:Double =45
    val lng_part:Double = lat_part*2
    
    val lat_scales = range_double(lat_min,lat_max+lat_part,lat_part)
    val lng_scales = range_double(lng_min,lng_max+lng_part,lng_part)
  
    println("lat- "+lat_scales.length+" : "+lat_scales.mkString(","))
    println("lng- "+lng_scales.length+" : "+lng_scales.mkString(","))
    
    for (i <- 0 to (lng_scales.length-2)){
      for (j <- (lat_scales.length-1) to 1 by -1){
        val topleft = (lng_scales(i),lat_scales(j))
        val bottomright = (lng_scales(i+1),lat_scales(j-1))
        println(s" cell($i,$j) =  $topleft , $bottomright ")
      }
    }
    
  }
  
  def range_double(min:Double,max:Double,step:Double): Array[Double] ={
    val size:Int = ((max-min)/step).toInt
    val res = new Array[Double](size)
    for (i <- 0 to (size-1)) {
      res(i) = min+(i*step)  
    }
    return res;
  }
}