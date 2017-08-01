package com.husrev.tle;

import org.joda.time.DateTime;

import com.github.amsacode.predict4java.GroundStationPosition;
import com.github.amsacode.predict4java.SatPos;
import com.github.amsacode.predict4java.SatelliteFactory;
import com.github.amsacode.predict4java.TLE;

import scala.Serializable;

public class SatelliteRealTime implements Serializable  {
	public String name="";
	public double x=0;
	public double y=0;
	public double z=0;
	public double v=0;
	public String t="";
	
	public SatelliteRealTime(String line0,String line1,String line2,DateTime time)
	{
		String[] TLE = {line0,line1,line2};
		TLE tle = new TLE(TLE);
		
		GroundStationPosition GROUND_STATION = new GroundStationPosition(39.928859, 32.858217,0);
		com.github.amsacode.predict4java.Satellite satellite = SatelliteFactory.createSatellite(tle);
		this.name = satellite.getTLE().getName();
		
		SatPos satellitePosition;
		satellitePosition = satellite.getPosition(GROUND_STATION,time.toDate());
		
		this.x = satellitePosition.getPositionECEF().getX();
		this.y = satellitePosition.getPositionECEF().getY();
		this.z = satellitePosition.getPositionECEF().getZ(); 
		this.v = satellitePosition.getVelocity().getW();
		this.t = time.toString();
		
		
	}
	
	
}
