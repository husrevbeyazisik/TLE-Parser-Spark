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

	static String tleFile = "C:/Users/husre/Desktop/cp.txt";
	
public static void main(String[] args) throws InterruptedException, IOException {
	//System.setProperty("hadoop.home.dir", "C:\\winutils");
	
	tleFile = args[0];
	
	
	SparkConf sparkConf =  new SparkConf()
			//.setMaster("local")
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
	

	URL url = new URL("http://10.1.219.49:2532/satellites/app/satellites/setPositionsRealTime");
	HttpURLConnection  connection;
	OutputStream outputStream;
	InputStream inputStream;
	
				while(true)
				{
					DateTime now = DateTime.now();
								satelliteList = satellitesDataset
								 .map(s -> new SatelliteRealTime(s[0],s[1],s[2],now))
								 .collect();
				
			
			
								connection = (HttpURLConnection) url.openConnection();
								connection.setDoOutput(true);
								connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
			
								
			
								outputStream = connection.getOutputStream();
								outputStream.write((gson.toJson(satelliteList)).getBytes("UTF-8"));
								outputStream.flush();
								inputStream = connection.getInputStream();
								outputStream.close();
						
			
						Thread.sleep(900);
				}
	}
	
}
