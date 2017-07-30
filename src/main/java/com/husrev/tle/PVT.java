package com.husrev.tle;

import scala.Serializable;

public class PVT implements Serializable {

	public double x;
	public double y;
	public double z;
	
	public double velocity;
	public String time;
	
	
	
	public PVT(double x, double y, double z, double velocity, String time) {
		super();
		this.x = x;
		this.y = y;
		this.z = z;
		this.velocity = velocity;
		this.time = time;
	}
	
	
	
}
