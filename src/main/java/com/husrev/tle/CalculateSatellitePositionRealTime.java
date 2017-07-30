package com.husrev.tle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.google.gson.Gson;

public class CalculateSatellitePositionRealTime {

	
public static void main(String[] args) throws InterruptedException, IOException {
	System.setProperty("hadoop.home.dir", "C:\\winutils");
	
	args = new String[4];
	args[0] = "C:/Users/husre/Desktop/3le.txt";
	
	
	SparkConf sparkConf =  new SparkConf()
			.setMaster("local")
			.setAppName("TLE Parser");
	
	JavaSparkContext sparkContext = new JavaSparkContext (sparkConf);
	sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");

	
	
	JavaRDD<String> satellitesDataset = sparkContext.textFile(args[0])
			.cache();
	
	
	Gson gson = new Gson();

	while(true)
	{
		DateTime now = DateTime.now();

			List<SatelliteRealTime> satelliteList = satellitesDataset.map(l -> l.split("\n") )
					 .map(s -> new SatelliteRealTime(s[0],s[1],s[2],now))
					 .collect();
			
			
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
			
			Thread.sleep(1000);
			

	}
	
}
	
}
