package com.husrev.tle;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

public class CalculateClosePassRDDold implements Serializable {
	static String positionFilePath;
	static int maxDistanceInKm = 10;
	
	static List<Satellite> satelliteList;
	static List<Positions> satellitePositionList;
	
	static JavaSparkContext sparkContext;
	
	public static void main(String[] args) {
		///////////////////
		/*args = new String[4];
		args[0] = "C:/Users/husre/Desktop/cp.txt";
		positionFilePath = "C:/Users/husre/Desktop/a";
		maxDistanceInKm = 400;
		System.setProperty("hadoop.home.dir", "C:\\winutils");*/
		////////////////////////
		positionFilePath = args[1];
		maxDistanceInKm = Integer.parseInt(args[2]);
		
		int stepSec = 900;
		int durationHour = 1;
		
		SparkConf sparkConf =  new SparkConf()
				//.setMaster("local")
				.setAppName("Close Pass RDD");
		
		sparkContext = new JavaSparkContext (sparkConf);
		sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");

		DateTime time = new DateTime("2017-08-08T15:00:15.127+03:00");
		System.out.println(time.toString());
		
		
		JavaRDD<String> satellitesDataset = sparkContext.textFile(args[0])
														.cache();
		
		
		
		JavaRDD<Positions> positions = satellitesDataset.map(l -> l.split("\n") )
									.map(l -> new Satellite(l[0],l[1],l[2],DateTime.now()))
									.map(s-> s.CalculatePositions(time, stepSec, durationHour));


		JavaPairRDD<Positions,Positions> positionsCartesian = positions.cartesian(positions);

		
		JavaRDD<String> calc = positionsCartesian.map(new Function<Tuple2<Positions, Positions>,String>(){

			@Override
			public String call(Tuple2<Positions, Positions> p) throws Exception {
				// TODO Auto-generated method stub
				String s = "";
				double distance;
				
				PVT pvt1, pvt2;
				int positionCount =  p._1().getPvt().size();
				
				for(int i=0; i < positionCount ; i++)
				{
					pvt1 = p._1().getPvt().get(i);
					pvt2 = p._2().getPvt().get(i);
					
					distance = Math.sqrt( Math.pow((pvt2.x-pvt1.x),2) 
				 			+ Math.pow((pvt2.y-pvt1.y),2) 
				 			+ Math.pow((pvt2.z-pvt1.z),2) );
					
					s += pvt1.time + " "+ p._1().name + "-"+ p._1().satelliteNumber + "-> "  + p._2().name + "-"+ p._2().satelliteNumber + " time1: " + pvt1.seqNo  
							+ " time2: " + pvt2.seqNo + " km "+ distance + "\n" ;
				}
			  return s;
			}
		});
		
		calc.saveAsTextFile(positionFilePath);
		
		
	}
	
	
	
}
