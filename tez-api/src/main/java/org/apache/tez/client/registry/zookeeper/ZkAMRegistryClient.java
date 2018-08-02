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

package org.apache.tez.client.registry.zookeeper;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Curator/Zookeeper impl of AMRegistryClient
 */
@InterfaceAudience.Public
public class ZkAMRegistryClient implements AMRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZkAMRegistryClient.class);

  private final Configuration conf;
  private CuratorFramework client;

  //Cache of known AMs
  private ConcurrentHashMap<String, AMRecord> amRecordCache = new ConcurrentHashMap<>();

  public ZkAMRegistryClient(final Configuration conf) throws Exception {
    this.conf = conf;
    init();
  }

  private void init() throws Exception {
    ZkConfig zkConf = new ZkConfig(this.conf);
    client = zkConf.createCuratorFramework();
    PathChildrenCache cache = new PathChildrenCache(client, zkConf.getZkNamespace(), true);
    client.start();
    cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    for (ChildData childData : cache.getCurrentData()) {
      AMRecord amRecord = getAMRecord(childData);
      amRecordCache.put(amRecord.getApplicationId().toString(), amRecord);
    }
    cache.getListenable().addListener(new ZkRegistryListener());
  }

  //Deserialize ServiceRecord from Zookeeper to populate AMRecord in cache
  private static AMRecord getAMRecord(final ChildData childData) throws IOException {
    byte[] data = childData.getData();
    String value = new String(data);
    RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
    ServiceRecord serviceRecord = marshal.fromJson(value);
    return new AMRecord(serviceRecord);
  }

  @Override public AMRecord getRecord(String appId) {
    return amRecordCache.get(appId);
  }

  @Override public List<AMRecord> getAllRecords() {
    return new ArrayList<>(amRecordCache.values());
  }

  //Callback for Zookeeper to update local cache
  private class ZkRegistryListener implements PathChildrenCacheListener {

    @Override public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event)
        throws Exception {
      Preconditions
          .checkArgument(client != null && client.getState() == CuratorFrameworkState.STARTED,
              "Curator client is not started");

      ChildData childData = event.getData();
      if (childData == null) {
        return;
      }
      AMRecord amRecord = getAMRecord(childData);
      LOG.info("Event {} for {}", event.getType(), amRecord.getApplicationId());
      switch (event.getType()) {
      case CHILD_ADDED:
        amRecordCache.put(amRecord.getApplicationId().toString(), amRecord);
        break;
      case CHILD_REMOVED:
        amRecordCache.remove(amRecord.getApplicationId().toString(), amRecord);
        break;
      default:
        // Ignore all the other events; logged above.
      }
    }
  }

  public void close() {
    client.close();
  }
}
