package com.husrev.tle;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReadClosePassFromFile {
	static String distanceFilePath;
	static int maxDistanceInKm = 10;
	
	public static void main(String[] args) {
		///////////////////
		args = new String[4];
		args[0] = "C:/Users/husre/Desktop/deneme.txt";
		distanceFilePath = "C:/Users/husre/Desktop/a";
		maxDistanceInKm = 400;
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		////////////////////////
		/*distanceFilePath = args[1];
		maxDistanceInKm = Integer.parseInt(args[2]);*/
		
		
		SparkConf sparkConf =  new SparkConf()
				.setMaster("local")
				.setAppName("Close Pass RDD");
		
		JavaSparkContext sparkContext = new JavaSparkContext (sparkConf);
		//sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");
		
		JavaRDD<String> file = sparkContext.textFile(distanceFilePath + "/part-*");

		JavaPairRDD<Tuple2<Integer,Integer>, Iterable<Tuple2<String,Double>>> distances = file.mapToPair(new PairFunction<String, Tuple2<Integer,Integer>, Iterable<Tuple2<String,Double>>>() {

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<String, Double>>> call(String line) throws Exception {
				List<Tuple2<String, Double>> distanceList = new ArrayList<Tuple2<String, Double>>();
				String[] splitted = line.split(",");
				
				int i;
				for(i = 2 ; i < splitted.length ; i=i+2)
					distanceList.add(new Tuple2(splitted[i].replaceAll("\\(|\\)","").replaceAll("\\[|\\]","").trim()
												,Double.valueOf(splitted[i+1].replaceAll("\\(|\\)","").replaceAll("\\[|\\]","").trim()) ) );

				
					return new Tuple2(new Tuple2(splitted[0].replaceAll("\\(|\\)","").replaceAll("\\[|\\]","").trim()
											,splitted[1].replaceAll("\\(|\\)","").replaceAll("\\[|\\]","").trim())
									, distanceList );
			}
		});
		
		
		for(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<String, Double>>> s : distances.collect())
		{
			System.out.println("satno1 : " + s._1()._1() + " satno2 : " + s._1()._2());
			
				for(Tuple2<String, Double> a : s._2())
					System.out.println("t : " + a._1() + " km: " + a._2());
		}
		
		
	
	}
	
}
