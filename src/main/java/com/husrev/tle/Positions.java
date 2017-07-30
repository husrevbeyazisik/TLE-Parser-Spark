package com.husrev.tle;

import java.util.ArrayList;
import java.util.List;

import scala.Serializable;

public class Positions implements Serializable {
	public String name;
	public List<PVT> pvt;

	
	
	public Positions(String name) {
		super();
		this.name = name;
		pvt = new ArrayList<PVT>();
	}
	
	
	public Positions() {
		pvt = new ArrayList<PVT>();
	}
	
	
	public void addPvt(double x,double y,double z,double velocity,String time)
	{
		this.pvt.add(new PVT(x,y,z,velocity,time));
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
