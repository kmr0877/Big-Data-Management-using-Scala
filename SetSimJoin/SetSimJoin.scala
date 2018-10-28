package comp9313.proj3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import collection.mutable.ArrayBuffer
import collection.mutable.HashSet
import org.apache.log4j.Logger
import org.apache.log4j.Level



object SetSimJoin {

  def f(idx: Int,id:Int, arr: Array[Int], t: Double): ArrayBuffer[(Int, (Int,Int, Array[Int]))] = {
    val ab = new ArrayBuffer[(Int, (Int,Int, Array[Int]))]()
    for(i <- 0 to  arr.length-1){
      ab.append( (arr(i), (idx,id, arr)) )
    }
    return ab;
  }

  def compute(a1: Array[Int], a2: Array[Int], key: Int): Double = {
    var sim = 0.0
    val dict = new HashSet[Int]
    for(i <- 0 to a1.size-1){
      dict+=a1(i)
    }
    var intersect = 0
    var union = a1.size
    var flag = false
    for(i <- 0 to a2.size-1){
      if(dict.contains(a2(i))){
        if(!flag){
          if(key == a2(i)) flag = true
          else  return -1
        }
        intersect = intersect + 1
      } else {
        union += 1
      }
    }
    sim = intersect.toDouble/union
    return sim


  }


  def sim(rec: Array[(Int,Int, Array[Int])], t: Double, key: Int): ArrayBuffer[(Int, Int, Double)] = {
    val pairs = new ArrayBuffer[(Int, Int, Double)]()
    for(i <- 0 to rec.size-1){
      for(j <- i+1 to rec.size-1){
       if(rec(i)._2!=rec(j)._2) {
          val rec1 = rec(i)._3
          val rec2 = rec(j)._3
          val similarity = compute(rec1, rec2, key)
          if (similarity >= t) {
            pairs.append((rec(i)._1, rec(j)._1, similarity))
          }
        }
      }
    }
    return pairs;
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val inputFile2 = args(1)
    val outputFolder = args(2)
    val threshold = args(3).toDouble

    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile).map(line=>line.split(" ")).map(arr => arr.map(_.toInt)).map( arr => (arr(0),1, arr.slice(1, arr.length).sortWith(_ < _)))
    val input2 = sc.textFile(inputFile2).map(line=>line.split(" ")).map(arr => arr.map(_.toInt)).map( arr => (arr(0),2, arr.slice(1, arr.length).sortWith(_ < _)))

    val records = input.union(input2)

    var i=0
    val pairs = records.flatMap{case(idx,id, arr) => f(idx, id,arr, threshold)}

    val joinres = pairs.groupByKey().flatMap{case(key, rec) => sim(rec.toArray, threshold, key)}.filter(x=>{
      if(x._1!=x._2){

        true
      }else{
        false
      }
    })
    val finalres = joinres.sortBy(_._2.toInt).sortBy(_._1.toInt).map(x => "(" + x._1 + ","+x._2+")\t"+BigDecimal(x._3).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble).repartition(1)

    finalres.saveAsTextFile(outputFolder)
  }
}



