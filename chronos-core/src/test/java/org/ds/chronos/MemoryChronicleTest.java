package org.ds.chronos;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.Chronos;
import org.ds.chronos.chronicle.MemoryChronicle;
import org.ds.support.TestBase;
import org.junit.Test;

public class MemoryChronicleTest extends TestBase {
	
	protected HColumn<Long,byte[]> getTestItem(long time) {
		return HFactory.createColumn(time, "Hello".getBytes());
	}

	protected List<HColumn<Long,byte[]>> getTestItemList(long startTime, long periodInMillis, int count) {
		List<HColumn<Long,byte[]>> result = new ArrayList<HColumn<Long,byte[]>>();
		for(int i = 0;i < count;i++) {
			result.add(getTestItem(startTime + (periodInMillis * i)));
		}
		return result;
	}
	
	@Test
	public void testAdd() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItem(0));
		Assert.assertEquals(1, chronicle.getNumEvents(0, System.currentTimeMillis()));
	}
	
	@Test
	public void testAddBatch() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 100));
		Assert.assertEquals(100, chronicle.getNumEvents(0, System.currentTimeMillis()));
	}
	
	@Test
	public void testRange() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 100));
		
		List<HColumn<Long, byte[]>> items = Chronos.toList(chronicle.getRange(1000, 5000));		
 
		Assert.assertEquals(5, items.size());
		Assert.assertEquals(1000, items.get(0).getName().longValue());
		Assert.assertEquals(5000, items.get(4).getName().longValue());
	}
	
	@Test
	public void testReverseRange() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 100));
		List<HColumn<Long, byte[]>> items = Chronos.toList(chronicle.getRange(5000, 1000));		
		Assert.assertEquals(5, items.size());
		Assert.assertEquals(5000, items.get(0).getName().longValue());
		Assert.assertEquals(1000, items.get(4).getName().longValue());
	}
	
	@Test
	public void testCount() {
		Chronicle chronicle = new MemoryChronicle();
		chronicle.add(getTestItemList(0, 1000, 100));
		Assert.assertEquals(50, chronicle.getNumEvents(1, 50000));
	}
	
}