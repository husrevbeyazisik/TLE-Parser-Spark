package com.husrev.tle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;



import scala.Serializable;

public class SqlTest {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .master("local")
				  .getOrCreate();

		
		Dataset<Row> df = spark.read().json("C:/Users/husre/Desktop/positions.json").toDF();
		Dataset<Row> parsed = df.select(df.col("name") , org.apache.spark.sql.functions.explode(df.col("pvt")).as("p"));


		
		
		List<Positions> positions = new ArrayList<Positions>();
		

		List<Row> name = parsed.select("name").collectAsList();
		List<Row> time =  parsed.select("p.time").collectAsList();
		List<Row> x =  parsed.select("p.x").collectAsList();
		List<Row> y =  parsed.select("p.y").collectAsList();
		List<Row> z =  parsed.select("p.z").collectAsList();
		List<Row> velocity =  parsed.select("p.velocity").collectAsList();
		
		
		
		String satName = name.get(0).toString().replaceAll("[\\[\\]]","");
		
		Positions p = new Positions();
		p.setName(satName);
		
		
		
		int i=0;
		for(Row n : name)
		{
			satName = n.toString().replaceAll("[\\[\\]]","");
			
			if(!satName.equals(p.getName()))
			{
				positions.add(p);
				p = new Positions();
				p.setName(satName);
			}
			
			p.pvt.add(new PVT(Double.parseDouble(x.get(i).toString().replaceAll("[\\[\\]]","")),
					Double.parseDouble(y.get(i).toString().replaceAll("[\\[\\]]","")),
					Double.parseDouble(z.get(i).toString().replaceAll("[\\[\\]]","")),
					Double.parseDouble(velocity.get(i).toString().replaceAll("[\\[\\]]","")),
					time.get(i++).toString().replaceAll("[\\[\\]]","")
					));
		}
		positions.add(p);
		
		
		for(Positions pos : positions)
		{
			System.out.println(pos.name);
			
				for(PVT pvt : pos.getPvt())
					System.out.println(pvt.time + "," + pvt.velocity + "," + pvt.x + "," + pvt.y + "," + pvt.z );
		}


	}

	

	
}
