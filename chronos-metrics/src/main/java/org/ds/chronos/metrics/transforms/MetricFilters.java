package org.ds.chronos.metrics.transforms;

import java.util.LinkedList;
import java.util.Queue;

import org.ds.chronos.metrics.Metric;
import org.ds.chronos.metrics.MetricSummary;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

/**
 * 
 * Factory for creating filters, maps, and aggregators.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricFilters {

	public static final Predicate<Metric> gte(final float v) {
		return new Predicate<Metric>() {

			public boolean apply(Metric metric) {
				return metric.getValue() >= v;
			}
		};
	}

	public static final Predicate<Metric> lte(final float v) {
		return new Predicate<Metric>() {

			public boolean apply(Metric metric) {
				return metric.getValue() <= v;
			}
		};
	}

	public static final Predicate<Metric> range(final float low, final float high) {
		return new Predicate<Metric>() {

			public boolean apply(Metric metric) {
				return metric.getValue() <= high && metric.getValue() >= low;
			}
		};
	}

	public static final Predicate<Metric> domain(final long t1, final long t2) {
		return new Predicate<Metric>() {

			public boolean apply(Metric metric) {
				return metric.getTime() <= t2 && metric.getValue() >= t1;
			}
		};
	}

	public static final Function<Metric, Metric> multiply(final float factor) {
		return new Function<Metric, Metric>() {

			public Metric apply(Metric metric) {
				metric.setValue(metric.getValue() * factor);
				return metric;
			}
		};
	}

	public static final Function<Metric, Metric> sma(final int amount) {
		return new Function<Metric, Metric>() {

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

			public Metric apply(Metric metric) {
				add(metric.getValue());
				metric.setValue(sum / count);
				return metric;
			}
		};
	}

	public static final Function<Metric, LinkedList<Metric>> buffer(final int size) {
		return new Function<Metric, LinkedList<Metric>>() {

			private LinkedList<Metric> buffer = new LinkedList<Metric>();

			public LinkedList<Metric> apply(Metric metric) {
				buffer.addLast(metric);
				if (buffer.size() > size) {
					buffer.removeFirst();
				}
				return buffer;
			}
		};
	}

	public static final Function<Iterable<Metric>, MetricSummary> summarize = new Function<Iterable<Metric>, MetricSummary>() {

		public MetricSummary apply(Iterable<Metric> metrics) {
			return new MetricSummary(metrics);
		}

	};
}
