package com.jobreadyprogrammer.spark;

import java.beans.Encoder;
import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Application {

	public static void main(String[] args) {
		
			
		SparkSession spark = SparkSession.builder()
		        .appName("Learning Spark SQL Dataframe API")
		        .master("local") // <--- need to remove this line to run on a live cluster
		        .getOrCreate();
		
		  
//		String redditFile = "s3n://your-bucket-name/Reddit_2011-large";
		
		String redditFile = "C:\\Users\\angel\\Desktop\\Coursera\\Apache_Spark_with_Java\\data\\Reddit-2007-small.json";
		
		    Dataset<Row> redditDf = spark.read().format("json")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .load(redditFile);
		    
		    redditDf = redditDf.select("body");
		    
		    Dataset<String> wordsDS = redditDf.flatMap((FlatMapFunction<Row, String>)
		    		row -> Arrays.asList(row.toString()
		    				.replace("\n", "")
		    				.replace("\r", "")
		    				.trim()
		    				.toLowerCase()
		    				.split(" ")).iterator(),
		    		Encoders.STRING());
		    
		    Dataset<Row> wordsDF = wordsDS.toDF();
		    
		    Dataset<Row> boringWordsDF = spark.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF();
		    
//		    wordsDF.except(boringWordsDF); would remove duplicates, need another solution
//		    use a left anti-join, removes boring words, but leaves duplicates
		    
		    wordsDF = wordsDF.join(boringWordsDF, wordsDF.col("value").equalTo(boringWordsDF.col("value")), "leftanti");
		    wordsDF = wordsDF.groupBy("value").count();
		    wordsDF.orderBy(desc("count")).show();
	}

	
	
}
