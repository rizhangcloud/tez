package org.apache.tez.frameworkplugins.zookeeper;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.registry.AMRegistryClient;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.apache.tez.client.registry.zookeeper.ZkFrameworkClient;
import org.apache.tez.frameworkplugins.ClientFrameworkService;

import java.util.Optional;

public class ZookeeperStandaloneClientFrameworkService implements ClientFrameworkService {
  @Override
  public Optional<FrameworkClient> createOrGetFrameworkClient(Configuration conf) {
    return Optional.of(new ZkFrameworkClient());
  }

  @Override
  public Optional<AMRegistryClient> createOrGetRegistryClient(Configuration conf) {
    try {
      ZkAMRegistryClient registry = ZkAMRegistryClient.getClient(conf);
      return Optional.of(registry);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
