package org.ds.chronos.aws;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

public class SimpleDBChronicle extends Chronicle {

	final AmazonSimpleDBClient client;
	final String key;

	public SimpleDBChronicle(AmazonSimpleDBClient client, String key) {
		super();
		this.client = client;
		this.key = key;
	}

	@Override
	public void add(ChronologicalRecord item) {
		add(Collections.singleton(item).iterator(), 0);
	}

	private ReplaceableItem getItem(ChronologicalRecord record) {
		ReplaceableItem item = new ReplaceableItem();
		item.setName(String.format("%013d", record.getTimestamp()));
		item.setAttributes(Collections.singleton(new ReplaceableAttribute("v", new String(record.getData()), true)));
		return item;
	}

	private final int PAGE_SIZE = 25;

	@Override
	public void add(Iterator<ChronologicalRecord> items, int pageSize) {
		BatchPutAttributesRequest request = new BatchPutAttributesRequest();
		request.setDomainName(key);

		int count = 0;
		Collection<ReplaceableItem> mapped = new LinkedList<ReplaceableItem>();
		while (items.hasNext()) {
			mapped.add(getItem(items.next()));

			if (++count % PAGE_SIZE == 0) {
				request.setItems(mapped);
				client.batchPutAttributes(request);
				mapped.clear();
			}
		}

		if (mapped.size() > 0) {
			request.setItems(mapped);
			client.batchPutAttributes(request);
		}
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(long t1, long t2, int pageSize) {
		SelectRequest request = new SelectRequest();
		request.setConsistentRead(true);
		request.setSelectExpression(getQuery("*", t1, t2));
		return new SimpleDBIterator(client, request);
	}

	private String getQuery(String projection, long t1, long t2) {
		if (t1 > t2) {
			return String.format(
			    "SELECT %s FROM `%s` WHERE itemName() >= '%013d' AND itemName() <= '%013d' ORDER BY itemName() DESC",
			    projection, key, t2, t1);
		}

		return String.format("SELECT %s FROM `%s` WHERE itemName() >= '%013d' AND itemName() <= '%013d'", projection, key,
		    t1, t2);
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		SelectRequest request = new SelectRequest();
		request.setConsistentRead(true);
		request.setSelectExpression(getQuery("count(*)", t1, t2));

		long count = 0;
		String token = null;

		do {
			if (token != null) {
				request.setNextToken(token);
			}

			SelectResult result = client.select(request);
			token = result.getNextToken();
			count += Integer.parseInt(result.getItems().get(0).getAttributes().get(0).getValue());
		} while (token != null);

		return count;
	}

	@Override
	public void delete() {
		client.deleteDomain(new DeleteDomainRequest().withDomainName(key));
	}

	@Override
	public void deleteRange(long t1, long t2) {
		// DeleteAttributesRequest request = new DeleteAttributesRequest();
		// request.setDomainName(key);
		// request.setItemName(itemName);
		// client.deleteAttributes(request);
	}

	public void createDomain() {
		client.createDomain(new CreateDomainRequest().withDomainName(key));
	}

}
