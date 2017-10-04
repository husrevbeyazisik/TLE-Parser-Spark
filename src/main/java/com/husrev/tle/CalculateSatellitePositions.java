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
	
	
	import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import scala.Serializable;

	public class CalculateSatellitePositions implements Serializable {

		static String positionFile;

		private static final long serialVersionUID = 1L;
		
		static String tleFile = "C:/Users/husre/Desktop/cp.txt";
		static String positionsFile =  "C:/Users/husre/Desktop/posalll.json";

		static DateTime startDate = new DateTime("2017-08-08T14:00");
		static int stepSec = 900;
		static int durationHour = 1;
		
		static List<Satellite> satelliteList;
		static List<Positions> satellitePositionList;
		
		public static void main(String[] args) {
			
			
			/*tleFile = args[0];
			positionsFile = args[1];
			startDate = new DateTime(args[2]);
			stepSec = Integer.parseInt(args[3]);
			durationHour = Integer.parseInt(args[4]);*/
			
			
			System.setProperty("hadoop.home.dir", "C:\\winutils");
		
			
			SparkConf sparkConf =  new SparkConf()
					.setMaster("local")
					.setAppName("TLE Parser");
			
			JavaSparkContext sparkContext = new JavaSparkContext (sparkConf);
			sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");
	
			
			
			JavaRDD<String> satellitesDataset = sparkContext.textFile(tleFile)
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
		        File file = new File(positionsFile);
		        
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
