package com.husrev.tle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CalculateSatellitePositionRealTime {

	
public static void main(String[] args) throws InterruptedException, IOException {
	System.setProperty("hadoop.home.dir", "C:\\winutils");
	
	args = new String[4];
	args[0] = "C:/Users/husre/Desktop/deneme.txt";
	
	
	SparkConf sparkConf =  new SparkConf()
			.setMaster("local")
			.setAppName("TLE Parser");
	
	JavaSparkContext sparkContext = new JavaSparkContext (sparkConf);
	sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");

	
	
	JavaRDD<String[]> satellitesDataset = sparkContext
			.textFile(args[0])
			.map(l -> l.split("\n") )
			.cache();
	
	
	Gson gson = new Gson();//Builder()
			//.serializeSpecialFloatingPointValues()
			//.create();

	List<SatelliteRealTime> satelliteList;
	
	
	URL url = new URL("http://localhost:2532/setPositionsRealTime");
	HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	connection.setDoOutput(true);
	connection.setRequestMethod("POST");
	connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
	connection.connect();
	
	OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
	
	while(true)
	{
		DateTime now = DateTime.now();
					satelliteList = satellitesDataset
					 .map(s -> new SatelliteRealTime(s[0],s[1],s[2],now))
					 .collect();
	




				    writer.write(gson.toJson(satelliteList));
			

			Thread.sleep(1000);
			

	}
	
	
	/*
	 
	 
	 			URL url = new URL("http://localhost:2532/setPositionsRealTime");
			URLConnection urlConnection = url.openConnection();
			urlConnection.setDoOutput(true);
			urlConnection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
			urlConnection.connect();
			

			OutputStream outputStream = urlConnection.getOutputStream();
			outputStream.write((gson.toJson(satelliteList)).getBytes("UTF-8"));
			outputStream.flush();
			InputStream inputStream = urlConnection.getInputStream();
			outputStream.close();
	 
	 
	 **/
	
	//sparkContext.close();
	//////
	/*List<SatelliteRealTime> satelliteList = satellitesDataset.map(l -> l.split("\n") )
			 .map(s -> new SatelliteRealTime(s[0],s[1],s[2],DateTime.now()))
			 .collect();
	
	for(SatelliteRealTime s: satelliteList)
	{
			System.out.print(s.x+",");
	}*/
	/////
	
}
	
}
