package org.ds.chronos.api;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.streams.TemporalIterator;
import org.ds.chronos.streams.TemporalStream;
import org.ds.chronos.streams.WindowIterator;
import org.junit.Assert;
import org.junit.Test;

public class WindowTest {

	protected ChronologicalRecord getTestItem(long time) {
		return new ChronologicalRecord(time, new byte[0]);
	}

	protected List<ChronologicalRecord> getTestItemList(long startTime, long periodInMillis, int count) {
		List<ChronologicalRecord> result = new ArrayList<ChronologicalRecord>();
		for (int i = 0; i < count; i++) {
			result.add(getTestItem(startTime + (periodInMillis * i)));
		}
		return result;
	}

	@Test(timeout = 1000)
	public void testIterator() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 1000));
		WindowIterator<ChronologicalRecord> itr = new WindowIterator<ChronologicalRecord>(chronicle.getRange(0, 1000000),
		    10000);

		int count = 0;

		while (itr.hasNext()) {
			count++;
			TemporalIterator<ChronologicalRecord> window = itr.next();
			int wcount = 0;
			while (window.hasNext()) {
				wcount++;
				window.next();
			}
			Assert.assertEquals(10, wcount);
		}

		Assert.assertEquals(100, count);
	}

	@Test(timeout = 1000)
	public void testGap() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 1000));
		chronicle.deleteRange(20000, 49999);
		WindowIterator<ChronologicalRecord> itr = new WindowIterator<ChronologicalRecord>(chronicle.getRange(0, 1000000),
		    10000);

		int count = 0;

		while (itr.hasNext()) {
			count++;
			TemporalIterator<ChronologicalRecord> window = itr.next();
			int wcount = 0;
			while (window.hasNext()) {
				wcount++;
				window.next();
			}
			if (window.getTimestamp() >= 20000 && window.getTimestamp() < 50000) {
				Assert.assertEquals(0, wcount);
			} else {
				Assert.assertEquals(10, wcount);
			}
		}

		Assert.assertEquals(100, count);
	}

	@Test(timeout = 1000)
	public void testStream() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 1000));
		Stream<TemporalStream<ChronologicalRecord>> stream = new WindowIterator<ChronologicalRecord>(
		    chronicle.getRange(0, 1000000), 10000).stream();
		Assert.assertEquals(1000, stream.mapToLong(s -> s.getStream().count()).sum());
	}

	@Test(timeout = 1000)
	public void testReduce() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 1000));
		Stream<TemporalStream<ChronologicalRecord>> stream = new WindowIterator<ChronologicalRecord>(
		    chronicle.getRange(0, 1000000), 10000).stream();
		Assert.assertEquals(100, stream.map(t -> t.getStream().reduce((x, y) -> x).get()).count());
	}

}
