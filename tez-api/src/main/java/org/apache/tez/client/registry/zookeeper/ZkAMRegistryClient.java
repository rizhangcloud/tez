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
import org.apache.tez.client.registry.AMRegistryClientListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
  * Curator/Zookeeper impl of AMRegistryClient
*/
@InterfaceAudience.Public
public class ZkAMRegistryClient implements AMRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZkAMRegistryClient.class);

  private final Configuration conf;

  //Cache of known AMs
  private ConcurrentHashMap<String, AMRecord> amRecordCache = new ConcurrentHashMap<>();
  private List<AMRegistryClientListener> listeners = new ArrayList<>();
  private CuratorFramework client;

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
  public static AMRecord getAMRecord(final ChildData childData) throws IOException {
    byte[] data = childData.getData();
    String value = new String(data);
    RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
    ServiceRecord serviceRecord = marshal.fromJson(value);
    return new AMRecord(serviceRecord);
  }

  @Override public AMRecord getRecord(String appId) {
  //Return a copy
    return new AMRecord(amRecordCache.get(appId));
  }

  @Override public List<AMRecord> getAllRecords() {
    return amRecordCache.values().stream()
        .map(record -> new AMRecord(record)).collect(Collectors.toList());
    }

  @Override public synchronized void addListener(AMRegistryClientListener listener) {
    listeners.add(listener);
  }

  //Callback for Zookeeper to update local cache
  private class ZkRegistryListener implements PathChildrenCacheListener {

  @Override public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event)
      throws Exception {
    Preconditions.checkArgument(client != null && client.getState() == CuratorFrameworkState.STARTED,
          "Curator client is not started");

    ChildData childData = event.getData();
    switch (event.getType()) {
      case CHILD_ADDED:
        if(isEmpty(childData)) {
          LOG.info("AppId allocated: {}", childData.getPath());
        } else {
          AMRecord amRecord = getAMRecord(childData);
          LOG.info("AM registered with data: {}", amRecord.toString());
          amRecordCache.put(amRecord.getApplicationId().toString(), amRecord);
          notifyOnAdded(amRecord);
        }
        break;
      case CHILD_UPDATED:
        if(isEmpty(childData)) {
          throw new RuntimeException("AM updated with empty data");
        } else {
          AMRecord amRecord = getAMRecord(childData);
          LOG.info("AM updated data: {}", amRecord.toString());
          amRecordCache.put(amRecord.getApplicationId().toString(), amRecord);
          notifyOnAdded(amRecord);
        }
        break;
      case CHILD_REMOVED:
        if(isEmpty(childData)) {
          LOG.info("Unused AppId unregistered: {}", childData.getPath());
        } else {
          AMRecord amRecord = getAMRecord(childData);
          LOG.info("AM removed: {}", amRecord.toString());
          amRecordCache.remove(amRecord.getApplicationId().toString(), amRecord);
          notifyOnRemoved(amRecord);
        }
        break;
      default:
        if(childData == null) {
          LOG.info("Ignored event {}", event.getType());
        } else {
          LOG.info("Ignored event {} for {}", event.getType(), childData.getPath());
        }
      }
    }

    private boolean isEmpty(ChildData childData) {
      return childData == null || childData.getData() == null || childData.getData().length == 0;
    }
  }

  private synchronized void notifyOnAdded(AMRecord record) {
    for(AMRegistryClientListener listener : listeners) {
      listener.onAdd(record);
    }
  }

  private synchronized void notifyOnRemoved(AMRecord record) {
    for(AMRegistryClientListener listener : listeners) {
      listener.onRemove(record);
    }
  }

  @Override
  public void close() {
    client.close();
  }
}