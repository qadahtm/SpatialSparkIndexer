package qadahtm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io._
import java.io.PrintWriter
import java.io.File
import scala.util.matching.Regex
import spray.json._

object Tareeg {

  def main(args: Array[String]): Unit = {

    //Read Path
    val node_path = args(0)
    val edge_path = args(1)
    val out_path = args(2)

    //Node_id,latitude,longitude
    val nodes = Source.fromFile(node_path, "UTF-8").getLines().drop(1)

    val nodehm = scala.collection.mutable.Map[String, JsObject]()
    var test = true;
    nodes.foreach { l =>
      {
        val arr = l.split(",")
        val id = arr(0)
        arr.drop(1)

        val njo = JsObject("id" -> JsNumber(id), "lat" -> JsNumber(arr(1)), "lng" -> JsNumber(arr(2)))
        nodehm.put(id, njo)
        //      if (test){
        //        arr(1) = "0"
        //        println(arr.mkString(","))
        //        test = false;
        //      }
      }
    }

    //Edge_id,Start_Node_id,End_Node_id,Tags
    val edges = Source.fromFile(edge_path, "UTF-8").getLines().drop(1)

    val out = new PrintWriter(new File(out_path))
    val utags = scala.collection.mutable.Map[String,(Int,Array[(JsObject,JsObject)])]()
    val otags = scala.collection.mutable.Map[String,Array[JsObject]]()
//    val outbn = new PrintWriter(new File(out_path))
    
    val joined = edges.foreach { e =>
      {
        val tagp = "\\{(\"((\\d|\\w)+:?)+\"=\"(\\d|\\w|\\s)+\")\\}".r
        val kv = "\"((\\d|\\s|\\w)+:?)+\"".r

        val earr = e.split(",")
        val tags = tagp.findAllIn(earr(3)).toArray
        var tkwv = ""
        val tagjsonArr = tags.flatMap { t =>
          {
            val pair = kv.findAllIn(t).toArray
            val kwk = pair(0).drop(1).dropRight(1)
            tkwv = pair(1).drop(1).dropRight(1)
            if (kwk == "name"){
              val sepair = (nodehm.get(earr(1)).get,nodehm.get(earr(2)).get)
               utags.get(tkwv) match {
                 case Some((c,ws)) =>{                   
                   val nl = ws ++ Array(sepair)
                   utags.update(tkwv, ((c+1),nl))
                 }
                 case None =>{
                   utags.put(tkwv, (1,Array(sepair)))
                 }
               }                         
            }
            if (kwk != "building") {
              Some(JsObject("key" -> JsString(kwk), "val" -> JsString(tkwv)))
            } else None
          }          
        }
        if (tkwv != ""){
          otags.put(tkwv, tagjsonArr)
        }
//        val json = JsObject("id" -> JsNumber(earr(0).toLong),
//          "startNode" -> nodehm.get(earr(1)).get,
//          "endNode" -> nodehm.get(earr(2)).get,
//          "tags" -> JsArray(tagjsonArr.toVector))
          
          
//          println("=========== Current Utags ===========")
//          utags.foreach{
//          case (k,ws) => {
//            print(k+":")
//            println(ws._2.mkString(","))
//          }
//          }
//        if (!tagjsonArr.isEmpty)
//          out.println(json.toString())
          
      }
      
    }
    
    
      val byname = out_path+".byname"
      
      utags.foreach{
        case (k,ws) => {
          
          var maxLat:BigDecimal = -100.0
          var maxLng:BigDecimal = -190.0
          var minLat:BigDecimal = 100.0
          var minLng:BigDecimal = 190.0
          
          ws._2.foreach(sep => {
            val snode = sep._1
            val enode = sep._2
            val clat = snode.fields.get("lat").get.asInstanceOf[JsNumber].value
            val clng = snode.fields.get("lng").get.asInstanceOf[JsNumber].value
            
            if (clat > maxLat) maxLat = clat
            if (clng > maxLng) maxLng = clng
            
            if (clat < minLat) minLat = clat
            if (clng < minLng) minLng = clng
            
          })
          
          val ftags = otags.getOrElse(k, Array())
          val fpbj = JsObject("name" -> JsString(k), 
              "mbr" -> JsObject("north" -> JsNumber(maxLat), "south" -> JsNumber(minLat),
                  "east" -> JsNumber(maxLng), "west" -> JsNumber(minLng)), "tags" -> JsArray(ftags.toVector))
          out.println(fpbj.toString())
        }
      }

  }

}