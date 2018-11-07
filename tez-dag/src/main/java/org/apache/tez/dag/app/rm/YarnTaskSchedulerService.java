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

package org.apache.tez.dag.app.rm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext.AMState;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext.AppFinalStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.common.ContainerSignatureMatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

/* TODO not yet updating cluster nodes on every allocate response
 * from RMContainerRequestor
   import org.apache.tez.dag.app.rm.node.AMNodeEventNodeCountUpdated;
    if (clusterNmCount != lastClusterNmCount) {
      LOG.info("Num cluster nodes changed from " + lastClusterNmCount + " to "
          + clusterNmCount);
      eventHandler.handle(new AMNodeEventNodeCountUpdated(clusterNmCount));
    }
 */
public class YarnTaskSchedulerService extends TaskScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(YarnTaskSchedulerService.class);
  @VisibleForTesting
  protected AtomicBoolean shouldUnregister = new AtomicBoolean(false);

  public YarnTaskSchedulerService(TaskSchedulerContext taskSchedulerContext) {
    super(taskSchedulerContext);
  }

  @Private
  @VisibleForTesting
  YarnTaskSchedulerService(TaskSchedulerContext taskSchedulerContext,
                           TezAMRMClientAsync<CookieContainerRequest> client) {
    super(taskSchedulerContext);
  }

  @Override
  public Resource getAvailableResources() throws ServicePluginException {
    return Resource.newInstance(2048, 2);
  }

  @Override
  public Resource getTotalResources() throws ServicePluginException {
    return Resource.newInstance(2048, 2);
  }

  @Override
  public int getClusterNodeCount() throws ServicePluginException {
    return 1;
  }

  @Override
  public void blacklistNode(NodeId nodeId) throws ServicePluginException {

  }

  @Override
  public void unblacklistNode(NodeId nodeId) throws ServicePluginException {

  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks, Priority priority, Object containerSignature, Object clientCookie) throws ServicePluginException {
    LOG.warn("Refusing to allocate task with Yarn! Replace with LLAP.");
    System.exit(-1);
  }

  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId, Priority priority, Object containerSignature, Object clientCookie) throws ServicePluginException {
    LOG.warn("Refusing to allocate task with Yarn! Replace with LLAP.");
    System.exit(-1);
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason, @Nullable String diagnostics) throws ServicePluginException {
    return false;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) throws ServicePluginException {
    return null;
  }

  @Override
  public void setShouldUnregister() throws ServicePluginException {

  }

  @Override
  public boolean hasUnregistered() throws ServicePluginException {
    return false;
  }

  @Override
  public void dagComplete() throws ServicePluginException {

  }

  class CookieContainerRequest extends ContainerRequest {
    CRCookie cookie;
    ContainerId affinitizedContainerId;

    public CookieContainerRequest(
            Resource capability,
            String[] hosts,
            String[] racks,
            Priority priority,
            CRCookie cookie) {
      super(capability, hosts, racks, priority);
      this.cookie = cookie;
    }

    public CookieContainerRequest(
            Resource capability,
            ContainerId containerId,
            String[] hosts,
            String[] racks,
            Priority priority,
            CRCookie cookie) {
      this(capability, hosts, racks, priority, cookie);
      this.affinitizedContainerId = containerId;
    }

    CRCookie getCookie() {
      return cookie;
    }

    ContainerId getAffinitizedContainer() {
      return affinitizedContainerId;
    }
  }

  static class CRCookie {
    // Do not use these variables directly. Can caused mocked unit tests to fail.
    private Object task;
    private Object appCookie;
    private Object containerSignature;

    CRCookie(Object task, Object appCookie, Object containerSignature) {
      this.task = task;
      this.appCookie = appCookie;
      this.containerSignature = containerSignature;
    }

    Object getTask() {
      return task;
    }

    Object getAppCookie() {
      return appCookie;
    }

    Object getContainerSignature() {
      return containerSignature;
    }
  }

  static class HeldContainer {

    enum LocalityMatchLevel {
      NEW,
      NODE,
      RACK,
      NON_LOCAL
    }

    final Container container;
    final private String rack;
    private long nextScheduleTime;
    private LocalityMatchLevel localityMatchLevel;
    private long containerExpiryTime;
    private CookieContainerRequest lastTaskInfo;
    private int numAssignmentAttempts = 0;
    private Object lastAssignedContainerSignature;
    final ContainerSignatureMatcher signatureMatcher;

    HeldContainer(Container container,
                  long nextScheduleTime,
                  long containerExpiryTime,
                  CookieContainerRequest firstTaskInfo,
                  ContainerSignatureMatcher signatureMatcher) {
      this.container = container;
      this.nextScheduleTime = nextScheduleTime;
      if (firstTaskInfo != null) {
        this.lastTaskInfo = firstTaskInfo;
        this.lastAssignedContainerSignature = firstTaskInfo.getCookie().getContainerSignature();
      }
      this.localityMatchLevel = LocalityMatchLevel.NODE;
      this.containerExpiryTime = containerExpiryTime;
      this.rack = RackResolver.resolve(container.getNodeId().getHost())
              .getNetworkLocation();
      this.signatureMatcher = signatureMatcher;
    }

    boolean isNew() {
      return lastTaskInfo == null;
    }

    String getRack() {
      return this.rack;
    }

    String getNode() {
      return this.container.getNodeId().getHost();
    }

    int geNumAssignmentAttempts() {
      return numAssignmentAttempts;
    }

    void incrementAssignmentAttempts() {
      numAssignmentAttempts++;
    }

    public Container getContainer() {
      return this.container;
    }

    public long getNextScheduleTime() {
      return this.nextScheduleTime;
    }

    public void setNextScheduleTime(long nextScheduleTime) {
      this.nextScheduleTime = nextScheduleTime;
    }

    public long getContainerExpiryTime() {
      return this.containerExpiryTime;
    }

    public void setContainerExpiryTime(long containerExpiryTime) {
      this.containerExpiryTime = containerExpiryTime;
    }

    public Object getLastAssignedContainerSignature() {
      return this.lastAssignedContainerSignature;
    }

    public CookieContainerRequest getLastTaskInfo() {
      return this.lastTaskInfo;
    }

    public void setLastTaskInfo(CookieContainerRequest taskInfo) {
      // Merge the container signatures to account for any changes to the container
      // footprint. For example, re-localization of additional resources will
      // cause the held container's signature to change.
      if (lastAssignedContainerSignature != null) {
        lastAssignedContainerSignature = signatureMatcher.union(
                lastAssignedContainerSignature,
                taskInfo.getCookie().getContainerSignature());
      } else {
        lastAssignedContainerSignature = taskInfo.getCookie().getContainerSignature();
      }
      lastTaskInfo = taskInfo;
    }

    public synchronized void resetLocalityMatchLevel() {
      localityMatchLevel = LocalityMatchLevel.NEW;
    }

    public synchronized void incrementLocalityMatchLevel() {
      if (localityMatchLevel.equals(LocalityMatchLevel.NEW)) {
        localityMatchLevel = LocalityMatchLevel.NODE;
      } else if (localityMatchLevel.equals(LocalityMatchLevel.NODE)) {
        localityMatchLevel = LocalityMatchLevel.RACK;
      } else if (localityMatchLevel.equals(LocalityMatchLevel.RACK)) {
        localityMatchLevel = LocalityMatchLevel.NON_LOCAL;
      } else if (localityMatchLevel.equals(LocalityMatchLevel.NON_LOCAL)) {
        throw new TezUncheckedException("Cannot increment locality level "
                + " from current NON_LOCAL for container: " + container.getId());
      }
    }

    public LocalityMatchLevel getLocalityMatchLevel() {
      return this.localityMatchLevel;
    }

    @Override
    public String toString() {
      return "HeldContainer: id: " + container.getId()
              + ", nextScheduleTime: " + nextScheduleTime
              + ", localityMatchLevel=" + localityMatchLevel
              + ", signature: "
              + (lastAssignedContainerSignature != null? lastAssignedContainerSignature.toString()
              : "null");
    }
  }
}
