package uk.bk.fraud.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import com.databricks.spark.csv.CsvContext
import org.apache.spark.rdd.RDD
import org.apache.spark._


object LogisticRegressionWS {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(794); 
  	
  	//val conf = new SparkConf()
  	//conf.setAppName("LogisticRegressionWS")
  	//conf.setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("credit.csv")
    
    val sc = new SparkContext("local", "test");System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(75); 
    
    var data = sc.textFile("/user/cloudera/fraud_data/credit.csv", 3);System.out.println("""data  : org.apache.spark.rdd.RDD[String] = """ + $show(data ));$skip(17); val res$0 = 
    data.cache();System.out.println("""res0: org.apache.spark.rdd.RDD[String] = """ + $show(res$0));$skip(59); 
    
    
    
    val vertices = sc.parallelize(1L to 5L);System.out.println("""vertices  : org.apache.spark.rdd.RDD[Long] = """ + $show(vertices ));$skip(27); 
  	println(vertices.count);$skip(16); 
  	println("5")}
}
