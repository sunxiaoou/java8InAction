/*
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
package lambdasinaction.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * A dummy replication endpoint that does nothing, for test use only.
 */
@InterfaceAudience.Private
public class DummyReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(DummyReplicationEndpoint.class);

  @Override
  public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override
  public UUID getPeerUUID() {
    return ctx.getClusterId();
  }

  @Override
  public WALEntryFilter getWALEntryfilter() {
    return null;
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    LOG.info("XO> replication length: " + replicateContext.getSize());
    LOG.info("XO> replication entries: " + replicateContext.getEntries().size());
    String WALId = replicateContext.getWalGroupId();
    LOG.info("XO> replication walGroupId: " + WALId);

    for (WAL.Entry entry : replicateContext.getEntries()) {
      LOG.info(entry.toString());
      for (Cell cell: entry.getEdit().getCells()) {
        KeyValue keyValue = (KeyValue) cell;
        Map<String, Object> map = keyValue.toStringMap();
        map.put("value", Bytes.toStringBinary(keyValue.getValueArray(), keyValue.getValueOffset(),
          keyValue.getValueLength()));
        map.put("type", keyValue.getType());
        LOG.info("XO> {}", map);
      }
    }

    return true;
  }

  @Override
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }
}
