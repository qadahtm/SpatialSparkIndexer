package qadahtm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


case class Cell(i:Int, j:Int, lat_range:Int) extends Equals

object OSMGridIndex {
  
  def main(args: Array[String]): Unit = {
    
    // syntax, OSMGridIndex <input_datafile> <out-dir> <lat_range> <proc_partitions> <final_partitions>
    
    if (args.length < 5){
      
      println("Usage: OSMGridIndex <input_datafile> <out-dir> <lat_range> <proc_partitions> <final_partitions>")
      exit()
    }
    
    
    val input_datafile = args(0)
    val out_dir = args(1)
    val lat_range = args(2).toInt
    val minPartitions = args(3).toInt
    val finalParitions = args(4).toInt
    
    
    
    
    
    val sparkConf = new SparkConf().setAppName("GridIndex")
    val sc = new SparkContext(sparkConf)
    
    
    
    val index = buildEmptyGridIndex(lat_range)    
    val partitions = Array.ofDim[RDD[(Cell,String)]](index.length*index.length)
    
    
    val data = sc.textFile(input_datafile, minPartitions)
    
    val paired = data.flatMap { x =>
      {
        try {
           val vals = x.split(",")
            val lat = vals(13).toDouble
            val lng = vals(14).toDouble
            // find cell in index
            
            findBucket(lat,lng,lat_range) match {
              case (Some(i),Some(j)) =>  {
                Some(new Cell(i,j,lat_range),x)
              }
              case _ => None
            }     
          
        } catch {
          case _:NumberFormatException => {
            // ignode maliformed input
            None   
          }
        }  
      }
    } 
    
    val cells = scala.collection.mutable.HashMap[String,RDD[String]]()
    
    for (i <- 0 to index.length-1){      
      for (j <- 0 to index.length-1){
        
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
        val name = s"cell-[$lat_min]-[$lat_max]-[$lng_min]-[$lng_max]"
        //println(name)
        cells.put(name, cellrdd)        
      }
    }
    
    cells.foreach(c => {
      val cname = c._1
      val crdd = c._2     
      
      println(cname+" has "+crdd.count()+" spatial objects")
      val rep_crdd = crdd.coalesce(finalParitions)
      rep_crdd.saveAsTextFile(out_dir+"-"+lat_range+"/"+cname)
    })
    
    
    println("finished building the index for "+input_datafile)    
    sc.stop()
    

  }
  
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
    for (i <- 0 to scales.length){
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


object OSMGridIndexTest {
  def main(args: Array[String]): Unit = {
    val ntest = args(0).toInt
    val lat_range = args(1).toInt
    
    OSMGridIndex.testBuildIndex(ntest, lat_range)
    
  }
}