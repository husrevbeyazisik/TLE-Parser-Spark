package com.husrev.tle;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import scala.Serializable;

public class CalculateClosePass implements Serializable {
	static String positionFilePath;
	static int maxDistanceInKm = 10;
	
	static List<Satellite> satelliteList;
	static List<Positions> satellitePositionList;
	
	static JavaSparkContext sparkContext;
	
	public static void main(String[] args) {
		///////////////////
	/*	args = new String[4];
		args[0] = "C:/Users/husre/Desktop/cp.txt";
		positionFilePath = "C:/Users/husre/Desktop/a";
		System.setProperty("hadoop.home.dir", "C:\\winutils");*/
		////////////////////////
		positionFilePath = args[1];
		maxDistanceInKm = Integer.parseInt(args[2]);
		
		int stepSec = 3600;
		int durationHour = 1;
		
		SparkConf sparkConf =  new SparkConf()
				.setMaster("local")
				.setAppName("TLE Parser 11:35");
		
		sparkContext = new JavaSparkContext (sparkConf);
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
		
	
	
		
		
		long satelliteCount;
		int stepCount = (durationHour * 3600) / stepSec;
		
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("ClosePass")
				  .master("local")
				  .getOrCreate();

		
		Dataset<Row> df = spark.read().json(positionFilePath+"/part-00000").toDF();
		satelliteCount = df.count();
		
		Dataset<Row> parsed = df.select(df.col("name") , org.apache.spark.sql.functions.explode(df.col("pvt")).as("p"));
		parsed.createOrReplaceTempView("positions");
		
		Dataset<Row> sqlDF = spark.sql("SELECT name,p.seqNo,p.x,p.y,p.z FROM positions");
		sqlDF.show();
	

		List<Row> currentDataset;
		String currentSatellite;
		int step = 0;
		Satellite sat1,sat2;
		double distance;
		
		List<Calculated> calculatedSats = new ArrayList<Calculated>();
		
			//Get satellite
			for(Row sat : df.collectAsList())
			{
				currentSatellite = sat.get(0).toString();
				
					//Get satellite steps
					for(step = 0; step < stepCount ;step ++)
					{
						//Get current satellite at the step moment
						currentDataset = spark.sql("SELECT name,p.seqNo,p.x,p.y,p.z,p.velocity,p.time "
								+ "FROM positions "
								+ "WHERE p.seqNo = "+step+" "
								+ "and name = '"+currentSatellite+"'").collectAsList();
						
						
									sat1 = new Satellite(currentDataset.get(0).get(0).toString(),
											Double.parseDouble(currentDataset.get(0).get(2).toString()),
											Double.parseDouble(currentDataset.get(0).get(3).toString()),
											Double.parseDouble(currentDataset.get(0).get(4).toString()),
											Double.parseDouble(currentDataset.get(0).get(5).toString()),
											currentDataset.get(0).get(6).toString() );
						
						//Get be compared satellites at the step moment
						currentDataset = spark.sql("SELECT name,p.seqNo,p.x,p.y,p.z,p.velocity,p.time "
								+ "FROM positions "
								+ "WHERE p.seqNo = "+step+" "
								+ "and not name = '"+currentSatellite+"'").collectAsList();

									for(Row row : currentDataset)
									{
										
										sat2 = new Satellite(row.get(0).toString(),
												Double.parseDouble(row.get(2).toString()),
												Double.parseDouble(row.get(3).toString()),
												Double.parseDouble(row.get(4).toString()),
												Double.parseDouble(row.get(5).toString()),
												row.get(6).toString() );
											
										

										Calculated calculate = new Calculated(sat1,sat2,step);
										Calculated calculateReverse = new Calculated(sat2,sat1,step);
										
										//Already calculate
										if(calculatedSats.contains(calculate) || calculatedSats.contains(calculateReverse))
											continue;
										else
											calculatedSats.add(calculate);

											
										
										
											distance = Math.sqrt( Math.pow((sat2.x-sat1.x),2) 
													 			+ Math.pow((sat2.y-sat1.y),2) 
													 			+ Math.pow((sat2.z-sat1.z),2) );
										
											if(distance <= maxDistanceInKm)
												System.out.println(sat1.time + " " +sat1.name + " & " + sat2.name + " -> " + distance +" km");
									}
									
					}
			}
		
	}
	
	
	private static void WriteJsonToFile()
	{
		Gson gson = new GsonBuilder()
				.serializeSpecialFloatingPointValues()
				.create();
		

	List<String> positionsTextFileFormat = new ArrayList();
	
	for(Positions p : satellitePositionList)
	{
		positionsTextFileFormat.add(gson.toJson(p));
		positionsTextFileFormat.add("\n");
	}
		
	JavaRDD<String> positionsTextFileFormatDD = sparkContext.parallelize(positionsTextFileFormat);
	
	positionsTextFileFormatDD.saveAsTextFile(positionFilePath);


	sparkContext.close();
	
	
	}

	
	static class Calculated{
		public Satellite sat1,sat2;
		public int step;
		
		
		public Calculated(Satellite sat1, Satellite sat2, int step) {
			this.sat1 = sat1;
			this.sat2 = sat2;
			this.step = step;
		}
		
		
		
		    @Override
		    public boolean equals(Object object)
		    {

		        if (object != null && object instanceof Calculated)
		        {
		        	Calculated calculated = (Calculated) object;
		 
		        	
		        	if((calculated.sat1.name.equals(this.sat1.name) && calculated.sat2.name.equals(this.sat2.name) && calculated.step == this.step) ||
		               (calculated.sat1.name.equals(this.sat2.name) && calculated.sat2.name.equals(this.sat1.name) && calculated.step == this.step))
		        			return true;	
		        }

		        return false;
		    }
		
		
		
	}
}
