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

package org.apache.tez.client.registry;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Represents an instance of an AM (DAGClientServer) in the AM registry
 */
@InterfaceAudience.Public
public class AMRecord {
  private ApplicationId appId;
  private String host;
  private int port;
  private final static String APP_ID_RECORD_KEY = "appId";
  private final static String HOST_RECORD_KEY = "host";
  private final static String PORT_RECORD_KEY = "port";

  public AMRecord(ApplicationId appId, String host, int port) {
    Preconditions.checkNotNull(appId);
    Preconditions.checkNotNull(host);
    this.appId = appId;
    this.host = host;
    this.port = port;
  }

  public AMRecord(ServiceRecord serviceRecord) {
    String serviceAppId = serviceRecord.get(APP_ID_RECORD_KEY);
    Preconditions.checkNotNull(serviceAppId);
    this.appId = ApplicationId.fromString(serviceAppId);
    String serviceHost = serviceRecord.get(HOST_RECORD_KEY);
    Preconditions.checkNotNull(serviceHost);
    this.host = serviceHost;
    String servicePort = serviceRecord.get(PORT_RECORD_KEY);
    this.port = Integer.parseInt(servicePort);
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object other) {
    if(other instanceof AMRecord) {
      AMRecord otherRecord = (AMRecord) other;
      return appId.equals(otherRecord.appId)
          && host.equals(otherRecord.host)
          && port == otherRecord.port;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return appId.hashCode() * host.hashCode() + port;
  }

  public ServiceRecord toServiceRecord() {
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(APP_ID_RECORD_KEY, appId);
    serviceRecord.set(HOST_RECORD_KEY, host);
    serviceRecord.set(PORT_RECORD_KEY, port);
    return serviceRecord;
  }

}
