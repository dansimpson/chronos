package org.ds.chronos.timeline.msgpack;

import java.util.ArrayList;
import java.util.List;

import org.ds.chronos.api.ChronosException;
import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.timeline.SimpleTimeline;
import org.ds.chronos.timeline.msgpack.MsgPackTimeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MsgPackTest {

	SimpleTimeline<TestObject> timeline;

	@Before
	public void setup() throws ChronosException {
		timeline = new MsgPackTimeline<TestObject>(new MemoryChronicle(), TestObject.class);
	}

	public TestObject buildObject(long time) {
		TestObject object = new TestObject();
		object.setId(4);
		object.setName("Dan");
		object.setAge(28);
		object.setWeight(171.0);
		object.setTimestamp(time);
		return object;
	}

	@Test
	public void testCodec() {
		TestObject object = buildObject(1000);
		timeline.add(buildObject(1000));
		TestObject other = timeline.query(0, 1000).first().get();
		Assert.assertNotNull(other);
		Assert.assertEquals(object.getId(), other.getId());
		Assert.assertEquals(object.getName(), other.getName());
		Assert.assertEquals(object.getAge(), other.getAge());
		Assert.assertEquals(object.getWeight(), other.getWeight(), 0.1);
		Assert.assertEquals(object.getTimestamp(), other.getTimestamp());
	}

	@Test
	public void testBatch() {
		List<TestObject> list = new ArrayList<TestObject>();
		for (int i = 0; i < 100; i++) {
			list.add(buildObject(i * 1000));
		}
		timeline.add(list);

		List<TestObject> other = timeline.query(0, 1000 * 100).toList();
		Assert.assertEquals(list.size(), other.size());
	}
}
