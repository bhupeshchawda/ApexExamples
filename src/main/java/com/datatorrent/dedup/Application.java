package com.datatorrent.dedup;

import java.util.Date;
import java.util.Random;

import org.apache.apex.malhar.lib.dedup.DeduperTimeBasedPOJOImpl;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

@ApplicationAnnotation(name = "DedupTestApp")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomDedupDataGenerator random = dag.addOperator("Input", RandomDedupDataGenerator.class);
    DeduperTimeBasedPOJOImpl dedup = dag.addOperator("Dedup", DeduperTimeBasedPOJOImpl.class);
    dedup.setKeyExpression("{$.key}");
    dedup.setTimeExpression("{$.date.getTime()}");
    dag.setInputPortAttribute(dedup.input, Context.PortContext.TUPLE_CLASS, TestPojo.class);

    Verifier unique = dag.addOperator("Unique", Verifier.class);
    Verifier duplicate = dag.addOperator("Duplicate", Verifier.class);
    Verifier expired = dag.addOperator("Expired", Verifier.class);

    dag.addStream("Input to Dedup", random.output, dedup.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Dedup to Unique", dedup.output, unique.input);
    dag.addStream("Dedup to Duplicate", dedup.duplicates, duplicate.input);
    dag.addStream("Dedup to Expired", dedup.expired, expired.input);
  }

  public static class RandomDedupDataGenerator extends BaseOperator implements InputOperator
  {
    private final long count = 10000;
    private long windowCount = 0;
    private long sequenceId = 1;
    private Random r = new Random();

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
      windowCount = 0;
    }

    @Override
    public void emitTuples()
    {
      TestPojo pojo;
      if (windowCount < count) {
        if(sequenceId % 100 < 5) {
          long duplicateSequence = r.nextInt((int)sequenceId);
          pojo = new TestPojo(duplicateSequence, new Date(duplicateSequence * 1000), sequenceId);
        } else {
          pojo = new TestPojo(sequenceId, new Date(sequenceId * 1000), sequenceId);
        }
        sequenceId++;
        output.emit(pojo);
      }
    }
  }

  public static class Verifier extends BaseOperator
  {
    long prevSequence = 0;
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        System.out.println("Processing: " + tuple);
        TestPojo pojo = (TestPojo)tuple;
        if (pojo.getSequence() < prevSequence) {
          System.out.println("Previous: " + prevSequence + " Next: " + pojo);
          throw new RuntimeException("Wrong sequence");
        }
        prevSequence = pojo.sequence;
      }
    };
  }
}
