/*
   Copyright 2015 - Thamir Qadah
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package qadahtm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.util.Precision
import scala.io.Source
import java.io.File
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import com.vividsolutions.jts.geom.Geometry
import java.util.logging.Logger
import com.vividsolutions.jts.io.ParseException


case class Cell(i:Int, j:Int, lat_range:Int) extends Equals

object OSMGridIndexRead {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GridIndex")
    val sc = new SparkContext(sparkConf)
    
     //Read Path
    val index_path = args(0)
    
    
    sc.stop()
  }  
}

object OSMGridIndex {
  
  def main(args: Array[String]): Unit = {
    
    // syntax, OSMGridIndex tiger|osm <input_datafile> <out-dir> <lat_range> <proc_partitions> <final_partitions>
    
    if (args.length < 6){
      printUsage()
    }
    
    
    
    val dataset = args(0)
    val input_datafile = args(1)
    val out_dir = args(2)
    val lat_range = args(3).toInt
    val minPartitions = args(4).toInt
    val finalParitions = args(5).toInt
    
    val indexbuilder : SparkIndexBuilder  = dataset match {
      case "tiger" => {
        new SparkIndexBuilder(input_datafile,out_dir,lat_range,
        minPartitions,finalParitions, Utils.tigerDSParseFunction(_))
      }
      case "osm" => {
        new SparkIndexBuilder(input_datafile,out_dir,lat_range,
        minPartitions,finalParitions, Utils.osmDSParseFunction(_))
    
      } 
      case _ => {
        printUsage
        new SparkIndexBuilder(input_datafile,out_dir,lat_range,
        minPartitions,finalParitions, zero(_))
      }
      
    }
    val sparkConf = new SparkConf().setAppName("GridIndex")
    val sc = new SparkContext(sparkConf)
    
    indexbuilder.build(sc)
    
    sc.stop()

  }
  
  def zero(line: String): Option[(Double,Double)] = None
  
  def printUsage() = {     
      println(""""Usage: OSMGridIndex tiger|osm <input_datafile> <out-dir> <lat_range>
                  <proc_partitions> <final_partitions>""")
      System.exit(0)
  }
  
  
}

abstract class GridIndexBuilder {
  
  def getCS() : (Double,Double,Double,Double) = {
    val (lat_min, lat_max) = (-90.0, 90.0)
    val (lng_min, lng_max) = (-180.0, 180.0)
    return (lat_min,lat_max,lng_min,lng_max)
  }
  
  def buildScales(lat_part_range:Int) : (Array[Double],Array[Double]) = {
    val (lat_min,lat_max,lng_min,lng_max) = getCS()
    val lat_part: Double = lat_part_range
    val lng_part: Double = lat_part * 2

    val lat_scales = range_double(lat_min, lat_max + lat_part, lat_part)
    val lng_scales = range_double(lng_min, lng_max + lng_part, lng_part)
    return (lat_scales,lng_scales)
  }
  
    def getMBR(i:Int,j:Int,lat_range:Int) : (Double,Double,Double,Double) = {
     val (lat_scales,lng_scales) = buildScales(lat_range);
     return (lat_scales(i),lat_scales(i+1),lng_scales(j),lng_scales(j+1))   
  }

  
  import scala.util.Random
  
  def getRandomLatLng(): (Double,Double) = {
    // Grid Setup
    val (lat_min,lat_max,lng_min,lng_max) = getCS()
    val lat = Random.nextDouble()*(lat_max*2)+lat_min
    val lng = Random.nextDouble()*(lng_max*2)+lng_min
    
    return (lat,lng)
  }
  
  def buildEmptyGridIndex(lat_range:Int) : Array[Array[Int]]= {
    val (lat_min,lat_max,lng_min,lng_max) = getCS()
    val (lat_scales,lng_scales) = buildScales(lat_range)
    val res = Array.ofDim[Int](lat_scales.length-1,lng_scales.length-1)
    return res;
  }
  
  def testBuildIndex(ntest:Int,lat_range: Int) = {
    
    val index = buildEmptyGridIndex(lat_range)
    
    for (i <- 1 to ntest){
      val (lat,lng) = getRandomLatLng()
      
      findBucket(lat,lng,lat_range) match {
        case (Some(i),Some(j)) => index(i)(j) = index(i)(j) + 1 
      }
      
//      println(s"grid cell = ("+lati+","+lngi+s") for ($lat,$lng)")
    }
    
    printIndex(index)
    
  }
  
  def printIndex(index:Array[Array[Int]]) : Unit = {
    for (i <- 0 to index.length-1){      
      println(index(i).mkString("\t"))
    }
  }
  
  def testFindBucket(ntest:Int) = {
    // Grid Setup
    val (lat_min, lat_max) = (-90.0, 90.0)
    val (lng_min, lng_max) = (-180.0, 180.0)

    val lat_part: Double = 45
    val lng_part: Double = lat_part * 2

    val lat_scales = range_double(lat_min, lat_max + lat_part, lat_part)
    val lng_scales = range_double(lng_min, lng_max + lng_part, lng_part)
    
    for (i <- 1 to ntest){
      val (lat,lng) = getRandomLatLng()      
      println(s"grid cell = ("++s") for ($lat,$lng)")
    }
   
  }
  
//  def findCell(lat:Double, lng:Double, lat_range:Int): Cell = {
//    val (i,j) = findBucket(lat, lng, lat_range)
//    return new Cell(i,j,lat,lng,lat_range)
//  }
//  
  def findBucket(lat:Double, lng:Double, lat_range:Int): (Option[Int],Option[Int]) = {
    val cs = getCS()
    val (lat_min,lat_max,lng_min,lng_max) = getCS()
    val (lat_scales,lng_scales) = buildScales(lat_range)
    return (findBucket1D(lat_scales, lat),findBucket1D(lng_scales, lng)) 
    
  }
 
  
  import scala.util.control.Breaks
  def findBucket1D(scales: Array[Double], value:Double): Option[Int] = {
    if (value < scales(0)) return None
    if (value > scales(scales.length-1)) return None
    for (i <- 0 to scales.length-1){
      if (i == scales.length-1 && scales(i) <= value) return Some(i)
      if (scales(i) <= value && scales(i+1) > value) return Some(i)
    }
    return None
  }

  def range_double(min: Double, max: Double, step: Double): Array[Double] = {
    val size: Int = ((max - min) / step).toInt
    val res = new Array[Double](size)
    for (i <- 0 to (size - 1)) {
      res(i) = min + (i * step)
    }
    return res;
  }
  
  
}

object Utils {
  
  val log = org.apache.log4j.Logger.getLogger(this.getClass.getName)
  
  private val reader = new com.vividsolutions.jts.io.WKTReader()
  private val writer = new com.vividsolutions.jts.io.WKTWriter()
  
  /**
   * Geotools based functions
   */
  
  def readWKT(wkt:String) : Option[Geometry] = {
    //log.info(s"readWKT: $wkt")
    val chars = new java.io.InputStreamReader(new ByteArrayInputStream(wkt.getBytes(StandardCharsets.UTF_8)))
    try{
      val res = reader.read(chars)
      chars.close()
      if (res == null) None
      else
        Some(res)      
    }
    catch{
      case e:Exception => {
        //log.warn(s"Exception in reading WKT($wkt) : "+e.getMessage)
        None
        }      
    }
  }
  
  
  /**
   * Parse functions reads a line of data and finds the representative point of that spatial object
   */
  
  /** 
   *  Spatial Hadoop's datasets functions
   */
  
  /**
   * Tiger lines include a point for each spatial object thus this is trivial
   */
  def tigerDSParseFunction(line:String) : Option[(Double,Double)] ={
    val vals = line.split(",")
    val lat = vals(13).toDouble
    val lng = vals(14).toDouble           
    Some((lat,lng))
  }
  
  
  /**
   * OSM datasets are WKT, we need to parse them and find a representative point.
   */
  def osmDSParseFunction(line:String) : Option[(Double,Double)] ={
    //println(s"parsing line: $line")
    
    val wkt = line.split("\t")(1)
    readWKT(wkt).flatMap { r => 
          if (!r.isEmpty()) Some((r.getCentroid.getX,r.getCentroid.getY))
          else None
          }    
  }
  
}

/**
 *  val input_datafile = args(0)
    val out_dir = args(1)
    val lat_range = args(2).toInt
    val minPartitions = args(3).toInt
    val finalParitions = args(4).toInt
 */
class SparkIndexBuilder(input_datafile:String,
                        out_dir:String,
                        lat_range:Int,
                        minPartitions:Int,
                        finalPartitions:Int,
                        lineParseFunc: String => Option[(Double,Double)]) extends GridIndexBuilder with Serializable{
  
  def build(sc:SparkContext) : Unit = {
    
   
    val index = buildEmptyGridIndex(lat_range)    
    val partitions = Array.ofDim[RDD[(Cell,String)]](index.length*index.length)
    
    
//    val data = sc.textFile(input_datafile, minPartitions)
    val data = sc.textFile(input_datafile).cache()
    
    val paired = data.flatMap { line =>
      {
        try {
          
          lineParseFunc(line).flatMap{
            case (lat,lng) =>{
               // find cell in index
              findBucket(lat,lng,lat_range) match {
              case (Some(i),Some(j)) =>  {
                Some(new Cell(i,j,lat_range),line)
              }
              case _ => None
              }     
            }
            case _ => None
          }
          
        } catch {
          case _:NumberFormatException => {
            // ignore maliformed input
            None   
          }
        }  
      }
    }.cache() 
    
    val cells = scala.collection.mutable.HashMap[String,RDD[String]]()
    
    for (i <- 0 to index.length-1){      
      for (j <- 0 to index(i).length-1){
        
        val cellrdd = paired.flatMap{
          case (c,s) => {
            if (c.i == i && c.j == j) {
              Some(s)
            }
            else None
          }
          case _ => None
        }
        
        val (lat_min,lat_max,lng_min,lng_max) = getMBR(i, j, lat_range)
//        val name = s"cell-[$lat_min]-[$lat_max]-[$lng_min]-[$lng_max]"
        val name = "cell_"+lat_min+"_"+lat_max+"_"+lng_min+"_"+lng_max
        //println(name)
        cells.put(name, cellrdd)        
      }
    }
    
    cells.map(c => {
      val cname = c._1
      val crdd = c._2     
      
      val rep_crdd = crdd.coalesce(finalPartitions,shuffle=true).cache()
      rep_crdd.saveAsTextFile(out_dir+"-"+lat_range+"/"+cname)
      println(cname+" has "+rep_crdd.count()+" spatial objects")
    })
    
    
    println("finished building the index for "+input_datafile)    
    
  }
  
}

object OSMGridIndexTest extends GridIndexBuilder{
  
  
  def main(args: Array[String]): Unit = {
   
    args(0) match {
      case "index" => {
        val ntest = args(1).toInt
        val lat_range = args(2).toInt
        
        testBuildIndex(ntest, lat_range)    
      } 
      case "wkt" => {
        
        val fname = args(1)
        testWKT(fname)
        
      }
      case _ => {
        println("error testing")
      }
      
    }
    
    
  }
  
  def testWKT(filename:String) = {
    val file = Source.fromFile(new File(filename))
    val lines = file.getLines().take(10)
    lines.foreach { l => {
      val wkt = l.split("\t")(1)
//      println(wkt)
      
      val res = Utils.readWKT(wkt)
      if (res.isDefined) println("x = "+res.get.getCentroid.getX+" , y = "+res.get.getCentroid.getY)
      
    } }
  }
}