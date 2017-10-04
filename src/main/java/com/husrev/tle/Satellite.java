package com.husrev.tle;

import scala.Serializable;


import org.joda.time.DateTime;

import com.github.amsacode.predict4java.*;
import com.github.amsacode.predict4java.GroundStationPosition;



public class Satellite implements Serializable   {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	TLE tle;
	Positions positions;


	//Line 1
	public String name;
	public int satelliteNumber;
	public char classification;
	public int internationalDesignatorLaunchYear;
	public int internationalDesignatorLaunchNumber;
	public String internationalDesignatorLaunchPiece;
	public int epochYear;
	public Double epoch;
	public double meanMotionFirstDerivative;
	public String meanMotionSecondDerivative;
	public String bStarDragTerm;
	public int ephemerisType;
	public int elementNumber;
	public int checksumLine1;
	
	//Line 2
	public double inclination;
	public double rightAscensionAscendingNode;
	public double eccentricity;
	public double argumentOfPerigee;
	public double meanAnomaly;
	public double meanMotion;
	public int revolutionNumberAtEpoch;
	public int checksumLine2;
	
	
	//PVT
	public double longitude;
	public double latitude;
	public double altitude;
	
	public double x;
	public double y;
	public double z;

	public double velocity;
	public String time;
	
	

	
	public Satellite(String line0,String line1,String line2,DateTime time)
	{
		
		//line0
		name = line0.substring(1).trim();
		
		//line1
		satelliteNumber =  Integer.parseInt(line1.substring(2, 7).trim());
		classification =  line1.charAt(7);
		internationalDesignatorLaunchYear = Integer.parseInt(line1.substring(9,11));
		internationalDesignatorLaunchNumber = Integer.parseInt(line1.substring(11,14));
		internationalDesignatorLaunchPiece = line1.substring(14,17);
		epochYear = Integer.parseInt(line1.substring(18,20));
		epoch = Double.parseDouble(line1.substring(20,32));
		meanMotionFirstDerivative = Double.parseDouble(line1.substring(33,43));
		meanMotionSecondDerivative = line1.substring(44,52);
		bStarDragTerm = line1.substring(53,61);
		ephemerisType = Character.getNumericValue(line1.charAt(62));
		elementNumber = Integer.parseInt(line1.substring(64,68).trim());
		checksumLine1 = Character.getNumericValue(line1.charAt(68));
		
		//line2 
		inclination = Double.parseDouble(line2.substring(8,16));
		rightAscensionAscendingNode = Double.parseDouble(line2.substring(17,25));
		eccentricity = Double.parseDouble(line2.substring(26,33));
		argumentOfPerigee = Double.parseDouble(line2.substring(34,42));
		meanAnomaly = Double.parseDouble(line2.substring(43,51));
		meanMotion = Double.parseDouble(line2.substring(52,63));
		revolutionNumberAtEpoch = Integer.parseInt(line2.substring(63,68).trim());
		checksumLine2 = Character.getNumericValue(line2.charAt(68));
		
		
		
		
		///PVT
		String[] TLE = {line0,line1,line2};
		
		GroundStationPosition GROUND_STATION = new GroundStationPosition(52.4670, -2.022, 200);
		
		tle = new TLE(TLE);

		com.github.amsacode.predict4java.Satellite satellite = SatelliteFactory.createSatellite(tle);

		
		SatPos satellitePosition = satellite.getPosition(GROUND_STATION,time.toDate());
		
				longitude = satellitePosition.getLongitude();
				latitude = satellitePosition.getLatitude();
				altitude = satellitePosition.getAltitude();
		
				x = satellitePosition.getPositionECEF().getX();
				y = satellitePosition.getPositionECEF().getY();
				z = satellitePosition.getPositionECEF().getZ();
				
				velocity = satellitePosition.getVelocity().getW();
				
				
	}
	

	
	public Satellite(String name,double x,double y,double z,double v,String t) {
		this.name = name;
		this.x = x;
		this.y = y;
		this.z = z;
		this.velocity = v;
		this.time = t;
	}



	public Positions CalculatePositions(DateTime time,int stepSec,int durationHour){
		GroundStationPosition GROUND_STATION = new GroundStationPosition(0, 0, 0);
		com.github.amsacode.predict4java.Satellite satellite = SatelliteFactory.createSatellite(tle);
		
		SatPos satellitePosition;
		satellitePosition = satellite.getPosition(GROUND_STATION,time.toDate());
		
		
		int stepCount = (durationHour * 3600) / stepSec;
		
		Positions p = new Positions(this.name,this.satelliteNumber);
		
		for(int i = 0; i < stepCount; i++)
		{
			satellitePosition = satellite.getPosition(GROUND_STATION,time.toDate());
			
			
			p.addPvt(i,
					satellitePosition.getPositionECEF().getX(), 
					satellitePosition.getPositionECEF().getY(), 
					satellitePosition.getPositionECEF().getZ(), 
					satellitePosition.getVelocity().getW(), 
					time.toString());
			
			
			time = time.plusSeconds(stepSec);
		}
	
	return p;
	}

	
	
	
}
