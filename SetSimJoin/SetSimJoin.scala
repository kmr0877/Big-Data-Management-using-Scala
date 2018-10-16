package comp9313.proj3
import java.io._
import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}

object SetSimJoin {
  def main(args: Array[String]) {

     var istrue = false
     var file1 = Source.fromFile(args(1)).getLines().toList
     var file2 = Source.fromFile(args(2)).getLines().toList
     var sum = 0d; ;
       val writer = new PrintWriter(new File(args(3)))
       
       for(i<-0 to file1.length-1){   
         for(k<-0 to file2.length-1){  
            var list1 = file1(i).split(" ").map(_.trim).toList  
            var list2 = file2(k).split(" ").map(_.trim).toList  
           
              var length = (list1.length+list2.length)-2;  
              var n = 0d
             for(j<-1 to list1.length-1){               
                for(l<-1 to list2.length-1){                
                  if(list1(j) == list2(l)){                     
                      n += 1 
                   }              
               }                
             }
              if((n/(length-2).toDouble) >= args(4).toDouble)
                writer.write("\n("+list1(0)+","+list2(0)+")	"+n/(length-2).toDouble)                       
       }        
     }     
     writer.close()
  }
}