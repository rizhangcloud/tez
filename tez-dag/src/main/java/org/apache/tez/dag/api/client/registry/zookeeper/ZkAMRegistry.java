/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api.client.registry.zookeeper;

import org.apache.tez.dag.api.client.registry.AMRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.zookeeper.ZkConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Curator/Zookeeper impl of AMRegistry (for internal use only)
 * Clients should use org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistryClient instead
 */
@InterfaceAudience.Private
public class ZkAMRegistry extends AMRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ZkAMRegistry.class);

  private CuratorFramework client = null;
  private String namespace = null;
  private List<AMRecord> amRecords = new ArrayList<>();

  public ZkAMRegistry() {
    super("ZkAMRegistry");
  }

  @Override
  public void serviceInit(Configuration conf) {
    ZkConfig zkConfig = new ZkConfig(conf);
    this.client = zkConfig.createCuratorFramework();
    this.namespace = zkConfig.getZkNamespace();
    LOG.info("AMRegistryZkImpl initialized");
  }

  @Override public void serviceStart() throws Exception {
    client.start();
    LOG.info("AMRegistryZkImpl started");
  }

  //Deletes from Zookeeper AMRecords that were added by this instance
  @Override public void serviceStop() throws Exception {
    for(AMRecord amRecord : amRecords) {
      delete(amRecord);
    }
    client.close();
    LOG.info("AMRegistryZkImpl shutdown");
  }

  //Serialize AMRecord to ServiceRecord and deliver the JSON bytes to
  //zkNode at the path:  <TEZ_AM_REGISTRY_NAMESPACE>/<appId>
  @Override public void add(AMRecord server) throws Exception {
    RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
    String json = marshal.toJson(server.toServiceRecord());
    client.
        create().
        creatingParentContainersIfNeeded().
        withMode(CreateMode.EPHEMERAL).
        forPath(namespace + "/" + server.getApplicationId().toString(),
            json.getBytes());
    amRecords.add(server);
  }

  @Override public void remove(AMRecord server) throws Exception {
    amRecords.remove(server);
    delete(server);
  }

  private void delete(AMRecord server) throws Exception {
    client.
        delete().
        forPath(namespace + "/" + server.getApplicationId().toString());
  }
}
