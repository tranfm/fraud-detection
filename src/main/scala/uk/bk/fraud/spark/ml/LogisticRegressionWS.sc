package uk.bk.fraud.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import com.databricks.spark.csv.CsvContext
import org.apache.spark.rdd.RDD
import org.apache.spark._


object LogisticRegressionWS {
  	
  	//val conf = new SparkConf()
  	//conf.setAppName("LogisticRegressionWS")
  	//conf.setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("credit.csv")
    
    val sc = new SparkContext("local", "test")
    
    var data = sc.textFile("/user/cloudera/fraud_data/credit.csv", 3)
    data.cache()
    
    
    
    val vertices = sc.parallelize(1L to 5L)
  	println(vertices.count)
  	println("5")
}