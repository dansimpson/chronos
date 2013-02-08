package org.ds.support;

import java.util.List;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.api.CassandraChronos;
import org.ds.chronos.api.ChronosException;
import org.junit.BeforeClass;

public class TestBaseWithCassandra extends TestBase {

  protected static Cluster cluster;
  protected static final String clusterName = "hydra";
  protected static final String keyspaceName = "chronostests";
  protected static Keyspace keyspace;

  @BeforeClass
  public static void setup() {
    if (cluster == null) {
      cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");

      KeyspaceDefinition ksDef = cluster.describeKeyspace(keyspaceName);

      if (ksDef != null) {
        List<ColumnFamilyDefinition> cfDefs = ksDef.getCfDefs();
        for (ColumnFamilyDefinition cfDef : cfDefs) {
          cluster.truncate(keyspaceName, cfDef.getName());
          cluster.dropColumnFamily(keyspaceName, cfDef.getName());
        }

        cluster.dropKeyspace(keyspaceName, true);
      }

      cluster
          .addKeyspace(HFactory.createKeyspaceDefinition(keyspaceName), true);

      keyspace = HFactory.createKeyspace(keyspaceName, cluster);
    }
  }

  public CassandraChronos getChronos(String cfName) throws ChronosException {
    return new CassandraChronos(cluster, keyspace, cfName);
  }

}
