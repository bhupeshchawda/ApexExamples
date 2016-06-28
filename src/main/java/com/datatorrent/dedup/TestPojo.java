package com.datatorrent.dedup;

import java.util.Date;

public class TestPojo
{
  private long key;
  private Date date;
  public long sequence;

  public TestPojo()
  {
  }

  public TestPojo(long key, Date date)
  {
    this.key = key;
    this.date = date;
  }

  public TestPojo(long key, Date date, long sequence)
  {
    this.key = key;
    this.date = date;
    this.sequence = sequence;
  }

  public long getKey()
  {
    return key;
  }

  public Date getDate()
  {
    return date;
  }

  public void setKey(long key)
  {
    this.key = key;
  }

  public void setDate(Date date)
  {
    this.date = date;
  }

  public long getSequence()
  {
    return sequence;
  }

  public void setSequence(long sequence)
  {
    this.sequence = sequence;
  }

  @Override
  public String toString()
  {
    return "TestPojo [key=" + key + ", date=" + date + ", sequence=" + sequence + "]";
  }

}
