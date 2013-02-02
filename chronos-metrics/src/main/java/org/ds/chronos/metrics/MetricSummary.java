package org.ds.chronos.metrics;

/**
 * 
 * Metric summary for aggregating values into a few basic
 * statistics.
 * 
 * @author Dan Simpson
 *
 */
public class MetricSummary {

  private Long time;
  private long duration;

  private long count;
  private double sum;

  private float min;
  private float max;

  private double sumSquared;

  public MetricSummary() {
    reset();
  }

  public void add(Metric metric) {
    if (time == null) {
      time = metric.getTime();
    }
    min = Math.min(metric.getValue(), min);
    max = Math.max(metric.getValue(), max);
    sum += metric.getValue();
    sumSquared += (metric.getValue() * metric.getValue());
    duration = metric.getTime() - time;
    ++count;
  }

  public MetricSummary clone() {
    MetricSummary result = new MetricSummary();
    result.time = time;
    result.duration = duration;
    result.count = count;
    result.sum = sum;
    result.max = max;
    result.min = min;
    result.sumSquared = sumSquared;
    return result;
  }

  public void reset() {
    time = null;
    duration = 0;
    count = 0;
    sum = 0;
    min = Float.MAX_VALUE;
    max = Float.MIN_NORMAL;
    sumSquared = 0;
  }

  public MetricSummary cloneAndReset() {
    if (time == null) {
      return null;
    }

    MetricSummary result = clone();
    reset();
    return result;
  }

  /**
   * @return the time
   */
  public long getTime() {
    return time;
  }

  /**
   * @return the duration
   */
  public long getDuration() {
    return duration;
  }

  /**
   * @return the count
   */
  public long getCount() {
    return count;
  }

  /**
   * @return the sum
   */
  public double getSum() {
    return sum;
  }

  /**
   * @return the min
   */
  public float getMin() {
    return min;
  }

  /**
   * @return the max
   */
  public float getMax() {
    return max;
  }

  /**
   * @return the sumSquared
   */
  public double getSumSquared() {
    return sumSquared;
  }

  /**
   * 
   * @return the mean of the aggregate
   */
  public double getMean() {
    return sum / count;
  }

  /**
   * 
   * @return
   */
  public double getStandardDeviation() {
    return Math.sqrt((sumSquared / count) - (getMean() * getMean()));
  }
}
