package com.husrev.tle;

	import java.io.BufferedWriter;
	import java.io.File;
	import java.io.FileWriter;
	import java.io.IOException;
	import java.text.DecimalFormat;
	import java.util.ArrayList;
	import java.util.List;

	import org.apache.spark.SparkConf;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;
	import org.apache.spark.api.java.function.Function;
	import org.apache.spark.sql.hive.*;
	
	
	import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import scala.Serializable;

	public class CalculateSatellitePositions implements Serializable {

		static String positionFile;
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		
		static List<Satellite> satelliteList;
		static List<Positions> satellitePositionList;
		
		public static void main(String[] args) {
			
			args = new String[4];
			args[0] = "C:/Users/husre/Desktop/pos.txt";
			positionFile = "C:/Users/husre/Desktop/positions.json";
			int stepSec = 5;
			int durationHour = 1;
			
			
			System.setProperty("hadoop.home.dir", "C:\\winutils");
		
			
			SparkConf sparkConf =  new SparkConf()
					.setMaster("local")
					.setAppName("TLE Parser");
			
			JavaSparkContext sparkContext = new JavaSparkContext (sparkConf);
			sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");
	
			
			
			JavaRDD<String> satellitesDataset = sparkContext.textFile(args[0])
															.cache();

			satelliteList = satellitesDataset.map(l -> l.split("\n") )
															 .map(l -> new Satellite(l[0],l[1],l[2],DateTime.now()))
															 .collect();
			
			
			satellitePositionList = sparkContext.parallelize(satelliteList)
														.map(s -> s.CalculatePositions(DateTime.now(), stepSec, durationHour))
														.collect();
		
			WriteJsonToFile();
			

			sparkContext.close();
		}
		
		
		private static void WriteJsonToFile()
		{
			/*Gson gson = new GsonBuilder()
					.serializeSpecialFloatingPointValues()
					.create();*/
			
			Gson gson = new Gson();
		
			
			
			
			try {
		        File file = new File(positionFile);
		        
		        if (!file.exists()) 
		            file.createNewFile();
		     
			
		        FileWriter fileWriter = new FileWriter(file, false);
		        BufferedWriter bWriter = new BufferedWriter(fileWriter);
		        
		        for(Positions p : satellitePositionList)
				{

					String json = gson.toJson(p);
					bWriter.write(json);
					bWriter.newLine();
				}
				
			bWriter.close();
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
