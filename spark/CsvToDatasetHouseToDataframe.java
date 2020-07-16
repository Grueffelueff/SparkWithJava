package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import com.jobreadyprogrammer.mappers.HouseMapper;
import com.jobreadyprogrammer.pojos.House;


public class CsvToDatasetHouseToDataframe {
	
	public void start() {
		
		SparkSession spark = SparkSession.builder()
		        .appName("CSV to dataframe to Dataset<House> and back")
		        .master("local")
		        .getOrCreate();
		
		
		 String filename = "src/main/resources/houses.csv";
		 
		    Dataset<Row> df = spark.read().format("csv")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .option("sep", ";")
		        .load(filename);
		    
		    System.out.println("This ist the dataframe");
//		    df.show(5);
//		    df.printSchema();
		    
		    Dataset<House> houseDS = df.map(new HouseMapper(), Encoders.bean(House.class));
		    
		    System.out.println("This is the Dataset");
//		    houseDS.show(5);
//		    houseDS.printSchema();
//		    
//		    Make the Dataset into a Dataframe again
		    
		    Dataset<Row> df2 = houseDS.toDF();
		    df2 = df2.withColumn("formatted_Date", concat(df2.col("vacantBy.date"), lit("_"), df2.col("vacantBy.year")));
		    df2.show(5);
		    
	}
	

	    
}

 
   