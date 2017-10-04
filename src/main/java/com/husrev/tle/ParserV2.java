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

import scala.Serializable;

public class ParserV2 {

	public static void main(String[] args) {
		
		args = new String[4];
		
		args[0] = "C:/Users/husre/Desktop/gok.txt";
		
		
		
		System.setProperty("hadoop.home.dir", "C:\\winutils");
	
		
		SparkConf sparkConf =  new SparkConf()
				.setMaster("local")
				.setAppName("TLE Parser");
		
		JavaSparkContext sparkContext = new JavaSparkContext (sparkConf);
		sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter","\n0");
		
		JavaRDD<String> satellitesDataset = sparkContext.textFile(args[0])
														.cache();

		List<Satellite> satelliteList = satellitesDataset.map(l -> l.split("\n") )
														 .map(l -> new Satellite(l[0],l[1],l[2],DateTime.now().minusDays(21).minusHours(4)))
														 .collect();
		
		

		for(Satellite l : satelliteList)
		{
			System.out.println(DateTime.now().minusDays(21).minusHours(4));
			System.out.println("Name : " + l.name);
			System.out.println("Lon : " + l.longitude);
			System.out.println("Lat : " + l.latitude);
			System.out.println("Alt : " + l.altitude);
			System.out.println("Velocity : " + l.velocity);
		}
		
	/*
		Tüm uyduların anlık konumlaır
	*/
		
		/*DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(3);
		
		try {
        File file = new File("C:/Users/husre/Desktop/pos.js");
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fileWriter = new FileWriter(file, false);
        BufferedWriter bWriter = new BufferedWriter(fileWriter);
        
        bWriter.write("var coordinats = [");
		
        int i=0;
		for(Satellite l : satelliteList)
		{
			String coordinats = "[" + l.x +"," + l.y + "," + l.z + "]";
			bWriter.write(coordinats);
			
			if(i++!=satelliteList.size()-1)
				bWriter.write(",");
		
			
		}
		  bWriter.write("];");
		   bWriter.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
	/*	for(Satellite l : satelliteList)
		{
			System.out.println(" ");
			
			
			System.out.println("Name : " + l.name);
			System.out.println("Satellite Number : " + l.satelliteNumber);
			System.out.println("Classification : " + l.classification);
			System.out.println("International Designator Launch Year : " + l.internationalDesignatorLaunchYear);
			System.out.println("International Designator Launch Number : " + l.internationalDesignatorLaunchNumber);
			System.out.println("International Designator Launch Piece : " + l.internationalDesignatorLaunchPiece);
			System.out.println("Epoch Year : " + l.epochYear);
			System.out.println("Epoch : " + l.epoch);
			System.out.println("Mean Motion First Derivative : " + l.meanMotionFirstDerivative);
			System.out.println("Mean Motion Second Derivative : " + l.meanMotionSecondDerivative);
			System.out.println("BSTAR drag term : " + l.bStarDragTerm);
			System.out.println("Ephemeris type : " + l.ephemerisType);
			System.out.println("Element number : " + l.elementNumber);
			//System.out.println("Checksum  : " + l.checksumLine1);
			

			System.out.println("Inclination : " + l.inclination);
			System.out.println("Right Ascension of the Ascending Node : " + l.rightAscensionAscendingNode);
			System.out.println("Eccentricity : " + l.eccentricity);
			System.out.println("Argument of Perigee : " + l.argumentOfPerigee);
			System.out.println("Mean Anomaly : " + l.meanAnomaly);
			System.out.println("Mean Motion : " + l.meanMotion);
			System.out.println("Revolution number at epoch : " + l.revolutionNumberAtEpoch);
			//System.out.println("Checksum  : " + l.checksumLine2);
			

			System.out.println("Lon : " + l.longitude);
			System.out.println("Lat : " + l.latitude);
			System.out.println("Alt : " + l.altitude);
			
			System.out.println("--------");
		}
*/
		sparkContext.close();
	}
	
}
