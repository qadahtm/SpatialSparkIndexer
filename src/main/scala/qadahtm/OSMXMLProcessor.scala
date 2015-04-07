package qadahtm

import scala.xml._
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsonFormat
import spray.json.JsNumber
import spray.json.JsArray
import java.io.PrintWriter
import java.io.File




object OSMXMLProcessor {
  
implicit object NodeFormat extends JsonFormat[Node] {
  def write(node: Node) =
    
    if (node.child.count(_.isInstanceOf[Text]) == 1)
      JsString(node.text)
    else
      JsObject(node.child.collect {
        case e: Elem => e.label -> write(e)
      }: _*)

    def read(jsValue: JsValue) = null // not implemented
  }
  
import NodeFormat._

   def main(args: Array[String]): Unit = {
     val in_path = args(0)
     val out_path = args(0)+".out"
     
     
     val osm = XML.loadFile(in_path)
     val out = new PrintWriter(new File(out_path))
     val nodes = (osm \\ "node")
     
// <node lon="-86.1516702" lat="39.7719723" uid="701297" user="erjiang" timestamp="2014-12-02T18:19:04Z" changeset="27185445" version="1" visible="true" id="3216144733">
//  <tag v="Marsh" k="name"/>
//  <tag v="supermarket" k="shop"/>
// </node>
     
     val fnodes = nodes.flatMap { n => {
       val tags = (n \ "tag")
       if (tags.size > 1){
        val id = JsString(n.attribute("id").get.head.toString())
        val lng = JsNumber(n.attribute("lon").get.head.toString().toDouble)
        val lat = JsNumber(n.attribute("lat").get.head.toString())
        val timestamp = JsString(n.attribute("timestamp").get.head.toString())
        val atags = tags.map { t => {
          val k = t.attribute("k").get.head.toString()
          val v = t.attribute("v").get.head.toString()
          JsObject("k" -> JsString(k), "v" -> JsString(v))
        } }
        val res = JsObject("nid" -> id, "lng" -> lng, "lat" -> lat, "ts" -> timestamp, "tags" -> JsArray(atags.toVector))
        out.println(res)
        println(res)
        Some(res)
       }
       else None
     }}
     
//     val jnodes = fnodes.map(NodeFormat.write(_).toString())
//     fnodes.foreach { println(_) }
     
     println("found "+fnodes.size+" with multiple tags from "+nodes.size)
     
   }

  def attributeEquals(name: String, value: String)(node: Node) : Boolean = 
  { 
      node.attribute(name).filter(_==value).isDefined
  }

}

object OSMGPXProcessor {
  
  def main(args:Array[String]) : Unit = {
     val in_path = args(0)
     val out_path = args(0)+".out"
     
     val public = new File(in_path+"/public")
     
     val gpxfiles = public.listFiles().filter { _.isDirectory() }.flatMap { _.listFiles().filter { _.isDirectory() }.flatMap { _.listFiles() } }
     
     gpxfiles.foreach { f => {
         val osm = XML.loadFile(f)
//         val out = new PrintWriter(new File(out_path))
         val nodes = (osm \\ "trkpt").foreach { t => {
           val lng = t.attribute("lon").get.head.toString()
           val lat = t.attribute("lat").get.head.toString()
           println(Array(lat,lng).mkString(","))
         } }

     } }     
     

     
//     val osm = XML.loadFile(in_path)
//     val out = new PrintWriter(new File(out_path))
//     val nodes = (osm \\ "node")
    
  }
  
}
