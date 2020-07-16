package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProducts {

	public static void main(String[] args) {
		
		
		SparkSession spark = SparkSession.builder()
		        .appName("Learning Spark SQL Dataframe API")
		        .master("local")
		        .getOrCreate();
	    
		String customers_file = "src/main/resources/customers.csv";
		 
	    Dataset<Row> customersDf = spark.read().format("csv")
	        .option("inferSchema", "true") // Make sure to use string version of true
	        .option("header", true)
	        .load(customers_file);
	    
	    String products_file = "src/main/resources/products.csv";
		 
	    Dataset<Row> productsDf = spark.read().format("csv")
	        .option("inferSchema", "true") // Make sure to use string version of true
	        .option("header", true)
	        .load(products_file); 
	    
	    String purchases_file = "src/main/resources/purchases.csv";

		 
	    Dataset<Row> purchasesDf = spark.read().format("csv")
	        .option("inferSchema", "true") // Make sure to use string version of true
	        .option("header", true)
	        .load(purchases_file); 
	    
	    System.out.println("*********all Data is loaded*************");
	    
	    Dataset<Row> combinedDF = purchasesDf.join(customersDf, purchasesDf.col("customer_id").equalTo(customersDf.col("customer_id")));
	    combinedDF = combinedDF.join(productsDf, combinedDF.col("product_id").equalTo(productsDf.col("product_id")));
//	    combinedDF.show();
	    combinedDF = combinedDF.drop("favorite_website")
	    		.drop(purchasesDf.col("customer_id"))
	    		.drop(purchasesDf.col("product_id"))
	    		.drop("product_id");
	    
//	    combinedDF.show();
	    
//	    combinedDF.groupBy("first_name").count().show();
	    
	    Dataset<Row> aggDF = combinedDF.groupBy("first_name", "last_name").agg(
	    		count("product_name").as("number_of_products"),
	    		max("product_price").as("most_expensive_product"),
	    		sum("product_price").as("total_spent")
	    		);
	    
	    aggDF.show();
	    
	    
	}

}
