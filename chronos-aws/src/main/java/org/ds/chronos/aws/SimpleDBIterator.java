package org.ds.chronos.aws;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ds.chronos.api.ChronologicalRecord;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

public class SimpleDBIterator implements Iterator<ChronologicalRecord> {

	private final AmazonSimpleDBClient client;
	private final SelectRequest request;

	private List<Item> buffer;
	private int index = 0;
	private boolean finished = false;

	public SimpleDBIterator(AmazonSimpleDBClient client, SelectRequest request) {
		this.client = client;
		this.request = request;
		this.buffer = new ArrayList<Item>();
	}

	private void load() {
		if (finished) {
			return;
		}

		SelectResult result = client.select(request);
		if (result.getNextToken() == null) {
			finished = true;
		} else {
			request.setNextToken(result.getNextToken());
		}

		index = 0;
		buffer = result.getItems();
	}

	@Override
	public boolean hasNext() {
		if (index < buffer.size()) {
			return true;
		}

		load();

		return index < buffer.size();
	}

	@Override
	public ChronologicalRecord next() {
		Item item = buffer.get(index++);

		long time = Long.parseLong(item.getName());
		byte[] data = item.getAttributes().get(0).getValue().getBytes();

		return new ChronologicalRecord(time, data);
	}

	@Override
	public void remove() {
	}

}
