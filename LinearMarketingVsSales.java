package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearMarketingVsSales {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("LinearRegressionExample")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> markVsSalesDf = spark.read()
			.option("header", "true")
			.option("inferSchema", "true")
			.format("csv")
			.load("C:\\Users\\angel\\Desktop\\Coursera\\Apache_Spark_with_Java\\data\\marketing-vs-sales.csv");
//		markVsSalesDf.show();
		
		Dataset<Row> mldf = markVsSalesDf.withColumnRenamed("sales", "label")
				.select("label", "marketing_spend");
		
		String[] featuresColumns = {"marketing_spend"};
		
		
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(featuresColumns)
				.setOutputCol("features");
		
		Dataset<Row> lblFeaturesDf = assembler.transform(mldf).select("label", "features");
		lblFeaturesDf = lblFeaturesDf.na().drop();
//		lblFeaturesDf.show();
		
//		Create a linear regression model object
		LinearRegression lr = new LinearRegression();
		LinearRegressionModel learningModel = lr.fit(lblFeaturesDf);
		
		learningModel.summary().predictions().show();
		System.out.println("R squared: " + learningModel.summary().r2());
		
		
			}
}
