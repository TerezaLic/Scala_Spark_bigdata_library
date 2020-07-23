package com.lickov.scala

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._   //Before version 2.0 variant:  Logger
import org.apache.spark._   //Before version 2.0 variant:  Context
import org.apache.spark.sql.SparkSession


object Data_Extract_csv extends App {
  
    /** A function that splits a line of input into (firstName, numFriends) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract some columns and convert to string/integers
      val name = fields(1).toString
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (name, numFriends)
  }
  
  /**     Initiate a Spark session.  */
          // Set the log level to only print errors
             Logger.getLogger("org").setLevel(Level.ERROR)
        
         
          // From version 2.0  with SparkSession,   used for option 2+3 with "spark.read.format"
               val spark = SparkSession.builder()
     								  .appName("Data_Extract_csv")
      								.master("local[*]")
      								.getOrCreate()  
      		
             
          // Before version 2.0 with SparkConf and SparkContext     
        
          // Create a SparkContext using every core of the local machine
          //   val sc = new SparkContext("local[*]", "Data_Extract_csv")     

          // SELECT 1 OPTION FROM 3 BELOW  
  
  /**  1. load into RDD   */
          /** 
          println("spark read all csv files from a directory into one RDD")
          
          // Load each line of the source data into an RDD.   From multiple files.
          val lines = sc.textFile("../fakefriends.csv,../fakefriends2.csv")
          // Use our parseLines function to convert to (age, numFriends) tuples
          val rdd = lines.map(parseLine)   
          */
  
  /**  2. load into DataFrame / DataSet  */
       
          println("spark read csv files from a directory into DataFrame")
          //better performance by specifying the schema
           val customSchema =StructType(Array(
                            StructField("id", StringType, true),
                            StructField("name", StringType, true),
                            StructField("some_number", IntegerType, true),
                            StructField("some_number_2", DoubleType, true))
)
           val DF = spark.read.format("csv")
                    .option("sep", ",")
                    .schema(customSchema)
                    //.option("inferSchema", "true") // take it from your csv file
                    //.option("header", "true") //if first row contains header
                    .csv("../fakefriends.csv","../fakefriends2.csv")
         
 /**            
  /** TEST */
  // rdd.foreach(println)
     DF.printSchema()
     * /
     */
}
