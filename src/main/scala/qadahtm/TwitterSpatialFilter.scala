package qadahtm

import scala.io.Source
import java.io.PrintWriter
import java.io.File

object TwitterSpatialFilter {
  
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
//    val bbox = args(0) match {
//      case "chicago" => {
//        BBox(-87.9013,41.781,-87.527,41.9449)        
//      }
//      case "indy" => {
//        BBox(-86.3416,39.6723,-86.0126,39.9398)
//      }
//    }
//    
//    val in_path = args(1)
//    val out_path = args(1)+".filtered."+args(0)
//    val out = new PrintWriter(new File(out_path), "UTF-8")
//    
//    
//    // [tweet_id], [created_at], [geo_lat], [geo_long], [user_id], [tweet_text]
//    val tweets = Source.fromFile(in_path,"UTF8").getLines()
//    
//    tweets.foreach { line => {
//      val tarr = line.split(",")
//      if (tarr.length == 6){
//        if (bbox.contains(tarr(2).toDouble, tarr(3).toDouble)) out.println(line)
//      }      
//    } }
    
    // test status update
    val cons = System.console()
    val sb = new StringBuilder()
    for(i <- 0 to 1000){
      sb.clear()
      val stri = ""+i
      for (j <- 1 to stri.length()) sb.append("\u0008")
      sb.append(stri)
      cons.printf("%s",sb.toString())
      Thread.sleep(300)
    }
    
  }

}