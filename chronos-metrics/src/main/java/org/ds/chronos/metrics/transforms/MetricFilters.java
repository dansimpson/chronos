package org.ds.chronos.metrics.transforms;

import java.util.LinkedList;
import java.util.Queue;

import org.ds.chronos.metrics.Metric;
import org.ds.chronos.metrics.MetricSummary;
import org.ds.chronos.timeline.stream.DataStreamAggregator.Aggregator;
import org.ds.chronos.timeline.stream.DataStreamFilter.FilterFn;
import org.ds.chronos.timeline.stream.DataStreamMap.MapFn;
import org.ds.chronos.timeline.stream.DataStreamTransform.TransformFn;

/**
 * 
 * Factory for creating filters, maps, and aggregators.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricFilters {

  public static final FilterFn<Metric> gte(final float v) {
    return new FilterFn<Metric>() {

      public boolean check(Metric metric) {
        return metric.getValue() >= v;
      }
    };
  }

  public static final FilterFn<Metric> lte(final float v) {
    return new FilterFn<Metric>() {

      public boolean check(Metric metric) {
        return metric.getValue() <= v;
      }
    };
  }

  public static final FilterFn<Metric> range(final float low, final float high) {
    return new FilterFn<Metric>() {

      public boolean check(Metric metric) {
        return metric.getValue() <= high && metric.getValue() >= low;
      }
    };
  }

  public static final FilterFn<Metric> domain(final long t1, final long t2) {
    return new FilterFn<Metric>() {

      public boolean check(Metric metric) {
        return metric.getTime() <= t2 && metric.getValue() >= t1;
      }
    };
  }

  public static final TransformFn<Metric> multiply(final float factor) {
    return new TransformFn<Metric>() {

      public Metric map(Metric metric) {
        metric.setValue(metric.getValue() * factor);
        return metric;
      }
    };
  }

  public static final TransformFn<Metric> sma(final int amount) {
    return new TransformFn<Metric>() {

      private Queue<Float> queue = new LinkedList<Float>();
      private float sum = 0;
      private int count = 0;

      private final void add(float val) {
        sum += val;
        queue.add(val);
        if (count >= amount) {
          sum -= queue.poll();
        } else {
          ++count;
        }
      }

      public Metric map(Metric metric) {
        add(metric.getValue());
        metric.setValue(sum / count);
        return metric;
      }
    };
  }

  public static final MapFn<Metric, LinkedList<Metric>> buffer(final int size) {
    return new MapFn<Metric, LinkedList<Metric>>() {

      private LinkedList<Metric> buffer = new LinkedList<Metric>();

      public LinkedList<Metric> map(Metric metric) {
        buffer.addLast(metric);
        if (buffer.size() > size) {
          buffer.removeFirst();
        }
        return buffer;
      }
    };
  }

  public static final Aggregator<Metric, MetricSummary> summarize(
      final String duration) {
    return new MetricSummarizer(duration);
  }
}
