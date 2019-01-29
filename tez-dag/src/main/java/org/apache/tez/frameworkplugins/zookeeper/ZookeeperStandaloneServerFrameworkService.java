package org.apache.tez.frameworkplugins.zookeeper;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistry;
import org.apache.tez.frameworkplugins.AmExtensions;
import org.apache.tez.frameworkplugins.ServerFrameworkService;

import java.util.Optional;

public class ZookeeperStandaloneServerFrameworkService implements ServerFrameworkService {
  private ZkAMRegistry amRegistry;

  @Override public synchronized Optional<AMRegistry> createOrGetAMRegistry(Configuration conf) {
    if(amRegistry == null) {
      try {
        amRegistry = new ZkAMRegistry(System.getenv(TezConstants.TEZ_AM_EXTERNAL_ID));
        amRegistry.init(conf);
        amRegistry.start();
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
    return Optional.of(amRegistry);
  }

  @Override public Optional<AmExtensions> createOrGetDAGAppMasterExtensions() {
    return Optional.of(new ZkStandaloneAmExtensions(this));
  }
}
