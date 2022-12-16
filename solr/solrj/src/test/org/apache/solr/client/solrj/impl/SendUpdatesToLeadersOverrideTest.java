/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.solrj.impl;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.update.processor.TrackingUpdateProcessorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the behavior of {@link CloudSolrClient#isUpdatesToLeaders}
 *
 * <p>nocommit: more explanation of how we test this
 */
public class SendUpdatesToLeadersOverrideTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String CONFIG = "tracking-updates";
  private static final String COLLECTION_NAME = "the_collection";

  private static final Set<String> LEADER_CORE_NAMES = new HashSet<>();
  private static final Set<String> PULL_REPLICA_CORE_NAMES = new HashSet<>();

  @AfterClass
  public static void cleanupExpectedCoreNames() throws Exception {
    LEADER_CORE_NAMES.clear();
    PULL_REPLICA_CORE_NAMES.clear();
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    assert LEADER_CORE_NAMES.isEmpty();
    assert PULL_REPLICA_CORE_NAMES.isEmpty();

    final int numNodes = 4;
    configureCluster(numNodes)
        .addConfig(
            CONFIG,
            getFile("solrj")
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve(CONFIG)
                .resolve("conf"))
        .configure();

    // create 2 shard collection with 1 NRT (leader) and 1 PULL replica
    assertTrue(
        CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG, 2, 1)
            .setPullReplicas(1)
            .setNrtReplicas(1)
            .process(cluster.getSolrClient())
            .isSuccess());

    final List<Replica> allReplicas =
        cluster.getSolrClient().getClusterState().getCollection(COLLECTION_NAME).getReplicas();
    assertEquals(
        "test preconditions were broken, each replica should have it's own node",
        numNodes,
        allReplicas.size());

    allReplicas.stream()
        .filter(Replica::isLeader)
        .map(Replica::getCoreName)
        .collect(Collectors.toCollection(() -> LEADER_CORE_NAMES));

    allReplicas.stream()
        .filter(r -> Replica.Type.PULL.equals(r.getType()))
        .map(Replica::getCoreName)
        .collect(Collectors.toCollection(() -> PULL_REPLICA_CORE_NAMES));

    log.info("Leader coreNames={}", LEADER_CORE_NAMES);
    log.info("PULL Replica coreNames={}", PULL_REPLICA_CORE_NAMES);
  }

  /**
   * Helper that stops recording and returns an unmodifiable list of the core names from each
   * recorded command
   */
  private static List<String> stopRecording(final String group) {
    return TrackingUpdateProcessorFactory.stopRecording(group).stream()
        .map(
            uc ->
                uc.getReq()
                    .getContext()
                    .get(TrackingUpdateProcessorFactory.REQUEST_NODE)
                    .toString())
        .collect(Collectors.toUnmodifiableList());
  }

  private static class RecordingResults {
    public final List<String> preDistribCoreNames;
    public final List<String> postDistribCoreNames;

    public RecordingResults(
        final List<String> preDistribCoreNames, final List<String> postDistribCoreNames) {
      this.preDistribCoreNames = preDistribCoreNames;
      this.postDistribCoreNames = postDistribCoreNames;
    }
  }

  /**
   * Given an {@link UpdateRequest} and a {@link SolrClient}, processes that request against that
   * client while {@link TrackingUpdateProcessorFactory} is recording, does some basic validation,
   * then passes the recorded <code>pre-distrib</code> and <code>post-distrib</code> coreNames to
   * the specified validators
   */
  private static RecordingResults assertUpdateWithRecording(
      final UpdateRequest req, final SolrClient client) throws Exception {

    TrackingUpdateProcessorFactory.startRecording("pre-distrib");
    TrackingUpdateProcessorFactory.startRecording("post-distrib");

    assertEquals(0, req.process(client, COLLECTION_NAME).getStatus());

    final RecordingResults results =
        new RecordingResults(stopRecording("pre-distrib"), stopRecording("post-distrib"));

    // post-distrib should never match any PULL replicas, regardless of request, if this fails
    // something is seriously wrong with our cluster
    assertThat(
        "post-distrib should never be PULL replica",
        results.postDistribCoreNames,
        everyItem(not(isIn(PULL_REPLICA_CORE_NAMES))));

    return results;
  }

  /**
   * Since {@link UpdateRequest#setParam} isn't a fluent API, this is a wrapper helper for setting
   * <code>shards.preference=replica.type:PULL</code> on the input req, and then returning that req
   */
  private static UpdateRequest prefPull(final UpdateRequest req) {
    req.setParam("shards.preference", "replica.type:PULL");
    return req;
  }

  // nocommit: - test CloudHttp2SolrClient as well

  // basic sanity check of expected default behavior
  public void testUpdatesDefaultToLeaders() throws Exception {
    try (CloudSolrClient client =
        new CloudLegacySolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .sendUpdatesOnlyToShardLeaders()
            .build()) {
      checkUpdatesDefaultToLeaders(client);
    }
  }

  /** nocommit: This test will fail until bug is fixed */
  public void testUpdatesWithShardsPrefPull() throws Exception {
    try (CloudSolrClient client =
        new CloudLegacySolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            // nocommit: builder name should never have been 'All', should have been 'Any'
            .sendUpdatesToAllReplicasInShard()
            .build()) {
      checkUpdatesWithShardsPrefPull(client);
    }
  }

  /**
   * Given a SolrClient, sends various updates and asserts expecations regarding default behavior:
   * that these requests will be initially sent to shard leaders, and "routed" requests will be sent
   * to the leader for that route's shard
   */
  private void checkUpdatesDefaultToLeaders(final CloudSolrClient client) throws Exception {
    assertTrue(
        "broken test, only valid on clients where updatesToLeaders=true",
        client.isUpdatesToLeaders());

    { // single doc add is routable and should go to a single shard
      final RecordingResults add =
          assertUpdateWithRecording(new UpdateRequest().add(sdoc("id", "hoss")), client);

      // single NRT leader is only core that should be involved at all
      assertThat("add pre-distrib size", add.preDistribCoreNames, hasSize(1));
      assertThat(
          "add pre-distrib must be leader",
          add.preDistribCoreNames,
          everyItem(isIn(LEADER_CORE_NAMES)));
      assertEquals(
          "add pre and post should match", add.preDistribCoreNames, add.postDistribCoreNames);

      // whatever leader our add was routed to, a DBI for the same id should go to the same leader
      final RecordingResults del =
          assertUpdateWithRecording(new UpdateRequest().deleteById("hoss"), client);
      assertEquals(
          "del pre and post should match", add.preDistribCoreNames, add.postDistribCoreNames);
      assertEquals(
          "add and del should have been routed the same",
          add.preDistribCoreNames,
          del.preDistribCoreNames);
    }

    { // DBQ should start on some leader, and then distrib to both leaders
      final RecordingResults record =
          assertUpdateWithRecording(new UpdateRequest().deleteByQuery("*:*"), client);

      assertThat("dbq pre-distrib size", record.preDistribCoreNames, hasSize(1));
      assertThat(
          "dbq pre-distrib must be leader",
          record.preDistribCoreNames,
          everyItem(isIn(LEADER_CORE_NAMES)));

      assertEquals(
          "dbq post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          new HashSet<>(record.postDistribCoreNames));
    }

    // nocommit: check an UpdateRequest with a mix of many doc adds DBIs, ...
    // nocommit: confirm exactly 2 requests, one to each leader

  }

  /**
   * Given a SolrClient, sends various updates using {#link #prefPull} and asserts expecations that
   * these requests will be initially sent to PULL replcias
   */
  private void checkUpdatesWithShardsPrefPull(final CloudSolrClient client) throws Exception {

    assertFalse(
        "broken test, only valid on clients where updatesToLeaders=false",
        client.isUpdatesToLeaders());

    { // single doc add...
      final RecordingResults add =
          assertUpdateWithRecording(prefPull(new UpdateRequest().add(sdoc("id", "hoss"))), client);

      // ...should start on (some) PULL replica, since we asked nicely
      assertThat("add pre-distrib size", add.preDistribCoreNames, hasSize(1));
      assertThat(
          "add pre-distrib must be PULL",
          add.preDistribCoreNames,
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));

      // ...then be routed to single leader for this id
      assertThat("add post-distrib size", add.postDistribCoreNames, hasSize(1));
      assertThat(
          "add post-distrib must be leader",
          add.postDistribCoreNames,
          everyItem(isIn(LEADER_CORE_NAMES)));

      // A DBI should also start on (some) PULL replica,  since we asked nicely.
      //
      // then it should be distributed to whaever leader our add doc (for the same id) was sent to
      final RecordingResults del =
          assertUpdateWithRecording(prefPull(new UpdateRequest().deleteById("hoss")), client);
      assertThat("del pre-distrib size", del.preDistribCoreNames, hasSize(1));
      assertThat(
          "del pre-distrib must be PULL",
          del.preDistribCoreNames,
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      assertEquals(
          "add and del should have same post-distrib leader",
          add.postDistribCoreNames,
          del.postDistribCoreNames);
    }

    { // DBQ start on (some) PULL replica, since we asked nicely, then be routed to all leaders
      final RecordingResults record =
          assertUpdateWithRecording(prefPull(new UpdateRequest().deleteByQuery("*:*")), client);

      assertThat("dbq pre-distrib size", record.preDistribCoreNames, hasSize(1));
      assertThat(
          "dbq pre-distrib must be PULL",
          record.preDistribCoreNames,
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));

      assertEquals(
          "dbq post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          new HashSet<>(record.postDistribCoreNames));
    }

    // nocommit: check an UpdateRequest with a mix of many doc adds DBIs, ...
    // nocommit: confirm we get exactly one request, no request splitting kicking in

  }
}
