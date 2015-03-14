package qadahtm

import crosby.binary._
import crosby.binary.Osmformat._
import crosby.binary.file._
import java.io.File
import java.io.FileInputStream
import scala.util.control.Breaks._


object OSMPBFExtractor {
  
  def main(args: Array[String]) = {
    println("Hello")
    val inputfile: String = args(0)
    val input = new FileInputStream(new File(inputfile))
    val brad = new ScalaTestBinaryParser()
    val bins = new BlockInputStream(input,brad)
    
    bins.process()
    
  }
  
}

class BreakException extends Exception

class ScalaTestBinaryParser extends BinaryParser {
  
   override def parseRelations(rels :java.util.List[Relation]) : Unit = {
          if (!rels.isEmpty)
              System.out.println("Got some relations to parse.");      
  }
  
  override def parseDense( nodes: DenseNodes) : Unit ={
            var lastId:Long=0;
            var lastLat:Long=0;
            var lastLon:Long=0;

            var i = 0
            var keyIndex = 0
            
            while (i < nodes.getIdCount){
              
              lastId += nodes.getId(i);
              lastLat += nodes.getLat(i);
              lastLon += nodes.getLon(i);
              val dLat = parseLat(lastLat)
              val dLon = parseLon(lastLon)
              
              println(f"Dense node, ID $lastId%d  @ $dLat%.6f , $dLon%.6f")
              
              // A single table contains all keys/values of all nodes.
              // Each node's tags are encoded in alternating <key_id> <value_id>.
              // A single stringid of 0 delimit when the tags of a node ends and the tags of the next node begin.
              val keys = scala.collection.mutable.Map[String,String]();
              
              try{
                 while (keyIndex < nodes.getKeysValsCount()) {
                 keyIndex = keyIndex + 1
                  val key_id = nodes.getKeysVals(keyIndex);
                  if (key_id == 0) {
                      throw new BreakException
                  } else if (keyIndex < nodes.getKeysValsCount()) {
                    keyIndex = keyIndex + 1
                      val value_id = nodes.getKeysVals(keyIndex);
                      val kstr =getStringById(key_id)
                      val vstr = getStringById(value_id)
                      keys.put(kstr, vstr);
                      println(f"$kstr%s : $vstr%s")
                  } else {
                      throw new Exception("Invalid DenseNodes key/values table");
                  }
              }
                
                
              }
              catch{
                case e:Exception =>{
                  //e.printStackTrace()
                  
                }
              }
              i = i +1
            }
         
  }
  
  override def parseNodes( nodes : java.util.List[Node]) :Unit = {
    var i = 0
    while (i < nodes.size()){
      val n = nodes.get(i)
      val nid = n.getId
      val nLat = parseLat(n.getLat)
      val nLon = parseLon(n.getLon)
      val sb = new StringBuilder()
       var j = 0
       sb.append("\n  Key=value pairs: ")
       while (j < n.getKeysCount){
        sb.append(getStringById(n.getKeys(i))).append("=")
                            .append(getStringById(n.getVals(i))).append(" ");
        j = j + 1
      }
      println(f"Regular node, ID $nid%d @ $nLat%.6f, $nLon%.6f , "+sb.toString())
      i = i + 1
    }
  }
  
  override def parseWays(ways : java.util.List[Way]) {
    var i = 0
    while (i < ways.size){
      val w = ways.get(i)
      println("Way ID = "+ w.getId)
      val sb = new StringBuilder()
      sb.append("  Nodes: ");
      var lastRef: Long = 0;
      var j = 0
      while (j < w.getRefsList.size()){
        var ref =  w.getRefsList.get(j)
        lastRef += ref
         sb.append(lastRef).append(" ");
        j = j + 1
      }
      sb.append("\n  Key=value pairs: ");
      
      j = 0
      while (j < w.getKeysCount){
        sb.append(getStringById(w.getKeys(i))).append("=")
                            .append(getStringById(w.getVals(i))).append(" ");
        j = j + 1
      }
      println(sb.toString)
    }
            
  }
  
  
  override def parse( header : HeaderBlock) : Unit = {
     println("Got header block.");
  }
  
  override def complete() : Unit = {
      println("Complete!");
  }
}

