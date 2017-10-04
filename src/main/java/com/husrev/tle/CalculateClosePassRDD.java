package com.husrev.tle;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

public class CalculateClosePassRDD implements Serializable {
	static String tleFile = "C:/Users/husre/Desktop/cp.txt";
	static String distanceFilePath =  "C:/Users/husre/Desktop/bb";

	static DateTime startDate = new DateTime("2017-08-08T14:00");
	static int stepSec = 3600;
	static int durationHour = 24;
	
	static List<Satellite> satelliteList;
	static List<Positions> satellitePositionList;
	
	static JavaSparkContext sparkContext;
	
	
	public static void main(String[] args) {

		//System.setProperty("hadoop.home.dir", "C:\\winutils");
		
		
		tleFile = args[0];
		distanceFilePath = args[1];
		startDate = new DateTime(args[2]);
		stepSec = Integer.parseInt(args[3]);
		durationHour = Integer.parseInt(args[4]);
		

		System.out.println("Spark app starts at " + DateTime.now().toString() +" with \ntle file :" + tleFile + 
				"\ndistance file :" + distanceFilePath + 
				"\nstartDate :" + startDate + 
				"\nstepSec :" + stepSec + 
				"\ndurationHour :" + durationHour 
				);
		
		SparkConf sparkConf =  new SparkConf()
				//.setMaster("local")
				.setAppName("Close Pass RDD");
		
		sparkContext = new JavaSparkContext (sparkConf);
		sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");

		JavaRDD<String> satellitesDataset = sparkContext.textFile(tleFile,100);
		

		List<Positions> p = satellitesDataset.map(l -> l.split("\n") )
				.map(l -> new Satellite(l[0],l[1],l[2],DateTime.now()))
				.map(s-> s.CalculatePositions(startDate, stepSec, durationHour)).collect();
		
		sparkContext.close();
		
		
		sparkContext = new JavaSparkContext (sparkConf);
		
		JavaRDD<Positions> positions =  sparkContext.parallelize(p);
		
		
		// Uydunun numarası ve bulunduğu koordinatlardan oluşan RDD
		JavaPairRDD<Integer,Iterable<PVT>> pList = positions.mapToPair(pos -> new Tuple2(pos.satelliteNumber,pos.pvt));
		
		// RDD that contains sat number and position
		JavaPairRDD<Integer,PVT> parsedValues = pList.flatMapValues(x -> x);

		// Prevent data repeats on cartesian
		JavaPairRDD<Tuple2<Integer, PVT>, Long> parsedValuesWithIndex = parsedValues.zipWithIndex();
		@SuppressWarnings("unchecked")
		JavaPairRDD< Tuple2<Integer,PVT>,Tuple2<Integer,PVT> > cartesian  = parsedValuesWithIndex
																			.cartesian(parsedValuesWithIndex)
																			.filter( f -> f._1()._2() < f._2()._2())
																			.mapToPair(mp -> new Tuple2(mp._1()._1() , mp._2()._1()));
		
		JavaPairRDD<Integer,CalculatedDistance> calculatedDistances = 
				cartesian.filter(f-> f._1()._2.seqNo == f._2()._2.seqNo) // Aynı zamanda karşılaşmaları için
						 .mapToPair( new PairFunction<Tuple2<Tuple2<Integer,PVT>,Tuple2<Integer,PVT>>, Integer, CalculatedDistance>() {

					@Override
					public Tuple2<Integer, CalculatedDistance> call(Tuple2<Tuple2<Integer, PVT>, Tuple2<Integer, PVT>> sats) throws Exception {
						
						PVT pvt1  = sats._1()._2();
						PVT pvt2  = sats._2()._2();
						
						double distance = Math.sqrt( Math.pow((pvt2.x-pvt1.x),2) 
					 			+ Math.pow((pvt2.y-pvt1.y),2) 
					 			+ Math.pow((pvt2.z-pvt1.z),2) );
						
						
						return new Tuple2(sats._1()._1(),new CalculatedDistance(sats._2()._1(),distance,sats._1()._2().time,sats._1()._2().seqNo));
					
					}
		});
		
		// Swap key-value pair, sort by key(CalculatedDistance) then swap key-value again
		JavaPairRDD<Integer, CalculatedDistance> sort =  calculatedDistances
														.mapToPair( mp -> new Tuple2<CalculatedDistance, Integer>(mp._2(),mp._1()))
														.sortByKey(new CalculatedDistanceComaprator())
														.mapToPair( mp -> new Tuple2<Integer,CalculatedDistance>(mp._2(),mp._1()));
		
		
		 JavaPairRDD<Tuple2<Integer,Integer>, Tuple2<String,Double>> seperated = sort.mapToPair(mp -> new Tuple2(new Tuple2(mp._1(),mp._2().satelliteNumber),new Tuple2(mp._2().time,mp._2().distance)));
				 																 
		
		 
		 JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Tuple2<String, Double>>> groupedDistances = seperated.groupByKey();
		 

		 //Db json
		 JavaRDD<String> jsonify = groupedDistances.map(f-> new GsonBuilder().serializeSpecialFloatingPointValues().create().toJson(f));
		 
		 jsonify.saveAsTextFile(distanceFilePath);
		 
		 sparkContext.close();


		 
		System.out.println("Spark app finish at " + DateTime.now().toString());
	
	}
	
	class DistanceRDDSchema implements Serializable
	{
		private Integer satelliteNumber1;
		private Integer satelliteNumber2;
		private String time;
		private Double distance;
		
		public Integer getSatelliteNumber1() {
			return satelliteNumber1;
		}
		public void setSatelliteNumber1(Integer satelliteNumber1) {
			this.satelliteNumber1 = satelliteNumber1;
		}
		public Integer getSatelliteNumber2() {
			return satelliteNumber2;
		}
		public void setSatelliteNumber2(Integer satelliteNumber2) {
			this.satelliteNumber2 = satelliteNumber2;
		}
		public String getTime() {
			return time;
		}
		public void setTime(String time) {
			this.time = time;
		}
		public Double getDistance() {
			return distance;
		}
		public void setDistance(Double distance) {
			this.distance = distance;
		}
		
		
	}
	

	static class CalculatedDistanceComaprator implements Comparator<CalculatedDistance>,Serializable
	{
		@Override
		public int compare(CalculatedDistance o1, CalculatedDistance o2) {
			
			return o1.distance.compareTo(o2.distance);
		}
		
	}
	
	static class CalculatedDistance implements Serializable{
		private Integer satelliteNumber;
		private Double distance;
		private String time;
		private Integer no;
		
		
		
		public CalculatedDistance(Integer satelliteNumber, Double distance, String time, Integer no) {
			super();
			this.satelliteNumber = satelliteNumber;
			this.distance = distance;
			this.time = time;
			this.no = no;
		}
		
		public Integer getSatelliteNumber() {
			return satelliteNumber;
		}
		public void setSatelliteNumber(Integer satelliteNumber) {
			this.satelliteNumber = satelliteNumber;
		}
		public Double getDistance() {
			return distance;
		}
		public void setDistance(Double distance) {
			this.distance = distance;
		}
		public String getTime() {
			return time;
		}
		public void setTime(String time) {
			this.time = time;
		}
		public Integer getNo() {
			return no;
		}
		public void setNo(Integer no) {
			this.no = no;
		}

		
		
	}
	
}
