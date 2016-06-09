package uk.bk.fraud.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


import com.databricks.spark.csv.CsvContext
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    


object FraudModel{

  def main(args: Array[String])  {
    
//    if (args.length < 1) {
//       System.err.println("Usage: Fraud Detection <datafile>")
//       System.exit(1)
//     }
//    
//     val datafile = args(0)
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("LogisticRegression and Binary Regression")
      .setMaster("local[*]")//.set("spark.ui.port","4141")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/user/cloudera/fraud_data/credit.csv")
    
        
    val excludedTrainingFeature = List("Creditability")
    val trainingFeatureIndexes = df.columns.diff(excludedTrainingFeature).map(df.columns.indexOf(_))

    val targetIndex = df.columns.indexOf("Creditability") 
    
    val data = df.rdd.map(r => LabeledPoint(
       r.getString(targetIndex).toDouble, // Get target value
       Vectors.dense(trainingFeatureIndexes.map(r.getString(_).toDouble).toArray) 
    )).cache()
    
    
    
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    
    
    val lbfgs = new LogisticRegressionWithLBFGS()
    val model = lbfgs.run(training)

    /*
     * Clear the threshold. Logistic Regression trains a model that outputs
     * the probability for a new example to be in the positive class. This can 
     * be used with a threshold in order to predict the unknown label of a new example.
     * Like we did in the SVM example, we clear the threshold, hence the raw probability
     * will be output by the model. This will allow us to plot the ROC curve. 
     */
    model.clearThreshold()

    //Compute the probability to be in the positive class for each of the test examples
    val probAndLabels = test.map { testExample =>
      val probability = model.predict(testExample.features)
      (probability, testExample.label)
    }

    //Compute the area under the ROC curve using the Spark's BinaryClassificationMetrics class
    val metrics = new BinaryClassificationMetrics(probAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC) //print the area under the ROC

    //Stop the Spark context
    sc.stop
    

  }

}
