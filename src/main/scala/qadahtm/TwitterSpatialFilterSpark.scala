package qadahtm

import scala.io.Source
import java.io.PrintWriter
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._


object TwitterSpatialFilterSpark {
  
  // Indy : http://api.openstreetmap.org/api/0.6/map?bbox=-86.3416,39.6723,-86.0126,39.9398
  // Chicago : http://api.openstreetmap.org/api/0.6/map?bbox=-87.9013,41.781,-87.527,41.9449
  // /api/0.6/map?bbox=left,bottom,right,top
  case class BBox(left:Double, bottom:Double, right:Double, top:Double){
    def overlaps2D(that:BBox): Boolean = {
      (overlaps1D(left, right, that.left, that.right) && overlaps1D(bottom, top, that.bottom, that.top))
    }
    
    def overlaps1D(s1:Double, e1:Double, s2:Double, e2:Double) : Boolean = {
      var res = false;
      if (s1 < s2 && e1 > s2) res = true;
      if (s1 < s2 && e1 > e2) res = true;
      if (s1 > s2 && s1 < e2) res = true;
      if (s1 > s2 && e1 < e2) res = true;
    
      res
    }
    
    def contains(lat:Double, lng:Double):Boolean = {
      (lat <= top && lat >= bottom) && (lng <= right && lng >= left)
    }
    
  }
  def main(args:Array[String]): Unit ={
    
    args.length match {
      case 6 =>{
        val conf = new SparkConf().setAppName("SpatialFilterTDS")//.setMaster(master)
        val sc = new SparkContext(conf)
        
        val left = args(3).toDouble
        val bottom = args(4).toDouble
        val right = args(5).toDouble
        val top = args(2).toDouble
        
        val inputfile = args(0)
        val outputfile = args(1)
        
        
        val tweets = sc.textFile(inputfile)
        
        val filterbox = BBox(left,bottom,right,top)
        val filtered = tweets.filter { line => {
          val tarr = line.split(",")
          if (tarr.length == 6){
            filterbox.contains(tarr(2).toDouble, tarr(3).toDouble)
          }      
          else false
        } }
       
       // write output 
        filtered.saveAsTextFile(outputfile)
        
      }
      case _ =>{
        println("Usage: TwitterSpatialFilterSpark inputfile outputfile top left bottom right")
        println("Note: top,left,bottom,right  are double values")
        println("Example: get tweets in indianapolis: TwitterSpatialFilterSpark inputfile outputfile 39.9398 -86.3416 39.6723 -86.0126")
        
      }
        
    }
    
  }

}