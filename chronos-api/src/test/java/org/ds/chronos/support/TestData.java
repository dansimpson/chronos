package org.ds.chronos.support;

import org.ds.chronos.api.Temporal;

public class TestData implements Temporal {

  public long time;
  public byte type;
  public double value;
  
	@Override
  public long getTimestamp() {
	  // TODO Auto-generated method stub
	  return time;
  }
}