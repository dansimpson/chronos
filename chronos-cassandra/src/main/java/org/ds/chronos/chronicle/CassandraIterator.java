package org.ds.chronos.chronicle;

import java.util.Iterator;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.HColumn;

import org.ds.chronos.api.ChronologicalRecord;


public class CassandraIterator implements Iterator<ChronologicalRecord> {

  private ColumnSliceIterator<String, Long, byte[]> upstream;
  
  public CassandraIterator(ColumnSliceIterator<String, Long, byte[]> upstream) {
    this.upstream = upstream;
  }
  
  @Override
  public boolean hasNext() {
    return upstream.hasNext();
  }

  @Override
  public ChronologicalRecord next() {
    HColumn<Long, byte[]> item = upstream.next();
    return new ChronologicalRecord(item.getName(), item.getValue());
  }

  @Override
  public void remove() {
    upstream.remove();
  }

}
