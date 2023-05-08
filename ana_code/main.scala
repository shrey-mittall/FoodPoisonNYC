import scala.io.Source
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.{Matrices, Matrix}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.stat.ANOVATest
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Hello extends App {
  override def main(args: Array[String]): Unit = {
    //stores the relative path to the csv file
    val filename = "./cleaned_csv_headers.csv"

    //then we create a source object based off of this filepath
    val source = Source.fromFile(filename)

    //we are going to go through the lines in the file, and split on comma
    val lines = source.getLines().map(_.split(",").map(_.trim))

    //converting file to list of arrays of fields
    var data = lines.toList

    //tidy up! make sure to close the file
    source.close()

    // Remove the first character and the last three characters from the last element of each array
    data = data.zipWithIndex.map {
      case (array, 0) => array // If the index is 0 (i.e., the first array), return the original array
      case (array, _) => {
        val lastElement = array.last
        val newLastElement = lastElement.substring(1, lastElement.length - 3)
        array.init :+ newLastElement
      }
    }

    //printing out the first 5 lines in data
    data.slice(0, 5).foreach(array => println(array.mkString("; ")))

    // Create a SparkSession
    //  val spark = SparkSession.builder().appName("MyApp").master("local[*]").getOrCreate()

    // Set the log level to WARN
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Convert the list of arrays to a DataFrame
    val df = data.toDF()

    // Add column names to the DataFrame
    val headerNames = Seq("Date", "Zip", "Borough", "Latitude", "Longitude")

    //this is splitting the one column DF into the 5
    val splitDF = df.selectExpr("transform(value, x -> split(x, ',')) as split_cols")

    //assigning the names for the new df columns
    var finalDF = splitDF.select(
      $"split_cols".getItem(0).alias("Date"),
      $"split_cols".getItem(1).alias("Zip"),
      $"split_cols".getItem(2).alias("Borough"),
      $"split_cols".getItem(3).alias("Latitude"),
      $"split_cols".getItem(4).alias("Longitude")
    )

    //removing the first row cause its just headers right now
    var row_to_delete = finalDF.first()
    finalDF = finalDF.filter(row => row != row_to_delete)

    //setting the latitude to decimal format
    val dfWithDecimalLat = finalDF.withColumn("Latitude", $"Latitude".cast("array<decimal(10,6)>")).selectExpr("Latitude[0] as Latitude", "Date", "Zip", "Borough", "Longitude")

    // setting the longitude to decimal format
    var newDf = dfWithDecimalLat.withColumn("Longitude", $"Longitude".cast("array<decimal(10,6)>")).selectExpr("Latitude", "Date", "Zip", "Borough", "Longitude[0] as Longitude")

    //reordering the dataframe
    newDf = newDf.select("Date", "Zip", "Borough", "Latitude", "Longitude")

    //casting the zip to an int
    newDf = newDf.withColumn("Zip", col("Zip")(0).cast("int"))

    // Show the DataFrame
    newDf.show(numRows = 20, truncate = false)

    //showing the dataframe schema to make sure datatype conversion worked
    newDf.printSchema()

    println(s"Number of columns in the DataFrame: ${newDf.columns.length}")

    //counting distinct zip codes
    val distinctZipCount = newDf.select("Zip").distinct().count()
    println(s"Number of distinct values in Zip column: $distinctZipCount")

    //printing distinct boroughs
    newDf.select("Borough").distinct().show()

    //calculating average lat and long
    val avgLongitude = newDf.agg(avg($"Longitude").cast("double")).collect()(0)(0)
    val avgLatitude = newDf.agg(avg($"Latitude").cast("double")).collect()(0)(0)

    //printing average lat and long
    println(s"Average Longitude: $avgLongitude")
    println(s"Average Latitude: $avgLatitude")

    //finding out the most common zip code int value
    val mostCommonZip = newDf.groupBy("Zip")
      .count()
      .sort(desc("count"))
      .select(col("Zip"))
      .first()
      .getInt(0)

    //printing the most common zip code value
    println(s"The most common Zip is: $mostCommonZip")

    //Calculating how many distinct zip codes there are
    val distinctZipDF = newDf.groupBy(col("Zip")).agg(count("*").as("RowCount"))

    //displaying the first 200 rows of that distinct zip code dataframe
    distinctZipDF.show(200)

    //displaying the original dataframe for comparison
    newDf.show()

    //now we will import the other file
    val otherFilename = "./inspections.csv"

    //ONLY REQUIRED WHEN RUNNING LOCALLY
    //  val Spark = SparkSession.builder().appName("Creating Dataframe").master("local[*]").getOrCreate()

    //creating a new dataframe with the restaurant inspections
    var inspectionsDF = spark.read.option("header", false).csv(otherFilename)

    //renaming all of the columns to be more readable
    inspectionsDF = inspectionsDF.withColumnRenamed("_c1", "zipcode")
    inspectionsDF = inspectionsDF.withColumnRenamed("_c4", "score")
    inspectionsDF = inspectionsDF.withColumnRenamed("_c3", "critical")

    //creating a separate DF to store the statistics we are going to work with
    val statsDF = inspectionsDF.groupBy("zipcode").agg(
      round(avg("score"), 2).alias("average_score"),
      round(sum(when(col("critical") === "Critical", 1).otherwise(0)) / count("*") * 100, 2).alias("critical_percentage")
    )

    //Dropping any rows with null average_score values
    val newStatsDF = statsDF.na.drop(Seq("average_score"))

    // Compute a zip grade based on average score per zipcode
    val gradesDF = newStatsDF.withColumn("zip_grade", when(col("average_score") < 14, 1.0).when(col("average_score") < 28, 2.0).otherwise(3.0))

    // Join the two dataframes to get the final result
    val resultDF = gradesDF.select("zipcode", "average_score", "zip_grade", "critical_percentage").orderBy("zipcode")

    // Select the desired columns and order by zipcode
    var outputDF = resultDF.select("zipcode", "average_score", "zip_grade", "critical_percentage").orderBy("zipcode")

    //renaming zipcode column to Zip so that we can commit an inner join on the two datasets on a shared column "Zip"
    outputDF = outputDF.withColumnRenamed("zipcode", "Zip")

    //Merging the two dataframes with an inner join
    var mergedDf = distinctZipDF.join(outputDF, Seq("Zip"), "inner")

    //Showing first 30 rows of merged dataframe
    mergedDf.show(30)

    //Calculating Pearson correlation coefficient for how many food poisoning cases are present in a zip code and what percentage of violations are critical
    val correlationVal = mergedDf.stat.corr("Rowcount", "critical_percentage", "pearson")

    //Print correlation coefficient(poor results)
    println(s"Pearson Coefficient between food poisoning cases count and critical violation percentage per zipcode: $correlationVal")
    val pearson1 = (s"Pearson Coefficient between food poisoning cases count and critical violation percentage per zipcode: $correlationVal")

    val correlation2Val = mergedDf.stat.corr("Rowcount", "average_score", "pearson")
    val pearson2 = (s"Pearson Coefficient between food poisoning cases count and average restaurant inspection score per zipcode: $correlation2Val")
    println(pearson2)

    val correlation3Val = mergedDf.stat.corr("Rowcount", "zip_grade", "pearson")
    val pearson3 = (s"Pearson Coefficient between food poisoning cases count and average grade per zipcode: $correlation3Val")
    println(pearson3)

    //Developing a multinomial logistic regression model
    val assembler = new VectorAssembler()
      .setInputCols(Array("RowCount", "average_score"))
      .setOutputCol("features")

    val output = assembler.transform(mergedDf).select($"zip_grade".as("label"), $"features")

    // Split the data into training and test sets
    val Array(trainingData, testData) = output.randomSplit(Array(0.7, 0.3))

    // Define the logistic regression model
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setElasticNetParam(0.5)
      .setFamily("multinomial")

    // Train the model
    val model = lr.fit(trainingData)

    // Make predictions on the test data
    val predictions = model.transform(testData)

    // Evaluate the model performance
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    // Excellent accuracy achieved
    val accuracyString = (s"Accuracy of the logistic regression model, predicting zip grade based on row_count\n and average score = $accuracy")
    println(accuracyString)

    statsDF.show(30)

    
    val rdd = spark.sparkContext.parallelize(Seq(accuracyString, pearson1, pearson2, pearson3))

    val outputDir2 = "dir1/output"
    // Write the RDD to a single file
    val tempDir = outputDir2 + "/temp"
    rdd.coalesce(1).saveAsTextFile(tempDir)

    // Move the output file to the final directory
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tempPath = new org.apache.hadoop.fs.Path(tempDir)
    val outputPath = new org.apache.hadoop.fs.Path(outputDir2)
    fs.rename(tempPath, outputPath)

    // Read the output file back in
    val outputRDD = spark.sparkContext.textFile(outputDir2)
  }
}