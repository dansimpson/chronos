package org.ds.chronos.timeline;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.ds.chronos.api.Temporal;
import org.ds.chronos.api.Timeline;
import org.ds.chronos.util.TimeFrame;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * A Timeline composed of other Timelines, each of which hold events for a given TimeFrame or "scope"
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class ConcatenatedTimeline<T extends Temporal> extends Timeline<T> {

	public static final class ScopedTimeline<T extends Temporal> {

		private TimeFrame scope;
		private Timeline<T> timeline;

		public ScopedTimeline(Timeline<T> timeline, TimeFrame scope) {
			this.timeline = timeline;
			this.scope = scope;
		}
	}

	final Collection<ScopedTimeline<T>> timelines;

	public ConcatenatedTimeline(ScopedTimeline<T>... timelines) {
		this(Arrays.asList(timelines));
	}

	public ConcatenatedTimeline(Collection<ScopedTimeline<T>> timelines) {
		this.timelines = timelines;
	}

	@Override
	public void add(Iterator<T> data, final int batchSize) {
		Map<ScopedTimeline<T>, List<T>> groups = new HashMap<ScopedTimeline<T>, List<T>>();

		while (data.hasNext()) {
			T item = data.next();

			for (ScopedTimeline<T> t : timelines) {
				if (t.scope.contains(item.getTimestamp())) {

					if (!groups.containsKey(t)) {
						groups.put(t, new LinkedList<T>());
					}
					groups.get(t).add(item);

					break;
				}
			}
		}

		for (Entry<ScopedTimeline<T>, List<T>> entry : groups.entrySet()) {
			entry.getKey().timeline.add(entry.getValue());
		}
	}

	@Override
	public void add(T item) {
		getTimeline(item.getTimestamp()).add(item);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<T> buildIterator(long t1, long t2, final int batchSize) {
		return Iterators.concat(Iterables.concat(
		    Iterables.transform(fit(t1, t2), new Function<ScopedTimeline<T>, Iterator<T>>() {

			    public Iterator<T> apply(ScopedTimeline<T> t) {
				    return t.timeline.buildIterator(t.scope.getStart(), t.scope.getEnd(), batchSize);
			    }
		    })).iterator());
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		long result = 0;
		for (ScopedTimeline<T> t : fit(t1, t2)) {
			result += t.timeline.getNumEvents(t.scope.getStart(), t.scope.getEnd());
		}
		return result;
	}

	@Override
	public boolean isEventRecorded(long time) {
		Timeline<T> timeline = getTimeline(time);
		if (timeline == null) {
			return false;
		}
		return timeline.isEventRecorded(time);
	}

	@Override
	public void deleteRange(long t1, long t2) {
		for (ScopedTimeline<T> t : fit(t1, t2)) {
			t.timeline.deleteRange(t.scope.getStart(), t.scope.getEnd());
		}
	}

	private Timeline<T> getTimeline(long time) {
		for (ScopedTimeline<T> timeline : timelines) {
			if (timeline.scope.contains(time)) {
				return timeline.timeline;
			}
		}
		return null;
	}

	private Iterable<ScopedTimeline<T>> filter(final long start, final long end) {

		return Iterables.filter(timelines, new Predicate<ScopedTimeline<T>>() {

			public boolean apply(ScopedTimeline<T> timeline) {
				return timeline.scope.intersects(start, end);
			}
		});
	}

	private Iterable<ScopedTimeline<T>> fit(final long start, final long end) {
		return Iterables.transform(filter(start, end), new Function<ScopedTimeline<T>, ScopedTimeline<T>>() {

			public ScopedTimeline<T> apply(ScopedTimeline<T> timeline) {
				return new ScopedTimeline<T>(timeline.timeline, timeline.scope.fit(start, end));
			}
		});
	}

}
