package com.husrev.tle;

import java.util.ArrayList;
import java.util.List;

import scala.Serializable;

public class Positions implements Serializable {
	public Integer satelliteNumber;
	public String name;
	public List<PVT> pvt;

	
	
	public Positions(String name) {
		super();
		this.name = name;
		pvt = new ArrayList<PVT>();
	}
	
	public Positions(String name,Integer satelliteNumber) {
		super();
		this.name = name;
		this.satelliteNumber = satelliteNumber;
		pvt = new ArrayList<PVT>();
	}
	
	
	public Positions() {
		pvt = new ArrayList<PVT>();
	}
	
	
	public void addPvt(int seqNo,double x,double y,double z,double velocity,String time)
	{
		this.pvt.add(new PVT(seqNo,x,y,z,velocity,time));
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public List<PVT> getPvt() {
		return pvt;
	}


	public void setPvt(List<PVT> pvt) {
		this.pvt = pvt;
	}
	
	
	
	
}



class PVT implements Serializable {


	public int seqNo;
	
	public double x;
	public double y;
	public double z;
	
	public double velocity;
	public String time;
	
	
	
	public PVT(int seqNo,double x, double y, double z, double velocity, String time) {
		super();
		this.seqNo = seqNo;
		this.x = x;
		this.y = y;
		this.z = z;
		this.velocity = velocity;
		this.time = time;
	}
	
	
	
}



