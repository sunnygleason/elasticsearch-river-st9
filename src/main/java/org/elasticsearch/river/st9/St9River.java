/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.st9;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileParser;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.g414.codec.lzf.LZFCodec;

/**
 *
 */
public class St9River extends AbstractRiverComponent implements River {
    private static final SmileFactory smileFactory = new SmileFactory();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final LZFCodec compressCodec = new LZFCodec();
    private static final String ST9_REDIS_FULLTEXT_PATTERN = "/1.0/f/*";
    private static final DateTimeFormatter format = ISODateTimeFormat
            .basicDateTimeNoMillis();

    private final Client client;
    private final String replayUrl;
    private final int replayTimeoutSecs;
    private final String indexName;
    private final String redisHost;
    private final int redisPort;
    private final int bulkSize;
    private volatile boolean closed = false;
    private volatile Thread consumerThread;
    private volatile Thread producerThread;
    private volatile Thread replayThread;
    private volatile JedisPool jedisPool;
    private volatile Jedis jedis;

    private BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(100000);

    @SuppressWarnings({ "unchecked" })
    @Inject
    public St9River(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("st9")) {
            Map<String, Object> redisSettings = (Map<String, Object>) settings
                    .settings().get("st9");
            replayUrl = XContentMapValues.nodeStringValue(
                    redisSettings.get("replayUrl"),
                    "http://localhost:8080/1.0/f/replay");
            replayTimeoutSecs = XContentMapValues.nodeIntegerValue(
                    redisSettings.get("replayTimeoutSecs"), 300);
            redisHost = XContentMapValues.nodeStringValue(
                    redisSettings.get("redisHost"), "localhost");
            redisPort = XContentMapValues.nodeIntegerValue(
                    redisSettings.get("redisPort"), 6379);
            indexName = XContentMapValues.nodeStringValue(
                    redisSettings.get("indexName"), "st9_index");
        } else {
            replayUrl = "http://localhost:8080/1.0/f/replay";
            replayTimeoutSecs = 300;
            redisHost = "localhost";
            redisPort = 6379;
            indexName = "st9_index";
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings
                    .settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(
                    indexSettings.get("bulk_size"), 100);
        } else {
            bulkSize = 100;
        }
    }

    @Override
    public void start() {
        jedisPool = new JedisPool(redisHost, redisPort);

        logger.info("creating st9 river, host [{}], port [{}]", redisHost,
                redisPort);

        producerThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(), "st9_river:producer").newThread(
                new Producer());
        producerThread.start();

        consumerThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(), "st9_river:consumer").newThread(
                new Consumer());

        consumerThread.start();

        replayThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(), "st9_river:replay").newThread(
                new Replay());
        replayThread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        logger.info("closing st9 river");
        closed = true;
        producerThread.interrupt();
        consumerThread.interrupt();
        replayThread.interrupt();
    }

    private class Producer extends BinaryJedisPubSub implements Runnable {
        public void run() {
            logger.info("producer thread starting...");
            while (true) {
                if (closed) {
                    break;
                }

                try {
                    jedis = jedisPool.getResource();
                    jedis.psubscribe(new Producer(),
                            ST9_REDIS_FULLTEXT_PATTERN.getBytes());
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("redis connection failed", e);

                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {
                            // ignore, if we are closing, we will exit later
                        }
                    }
                } finally {
                    jedisPool.returnResource(jedis);
                }
            }
            logger.info("producer thread exiting...");
        }

        @Override
        public void onMessage(byte[] topic, byte[] message) {
            if (logger.isTraceEnabled()) {
                logger.trace("received message topic={}, body={}", topic,
                        message);
            }

            queue.add(message);
        }

        @Override
        public void onPMessage(byte[] pattern, byte[] topic, byte[] message) {
            if (logger.isTraceEnabled()) {
                logger.trace("received message pattern={}, topic={}, body={}",
                        pattern, topic, message);
            }

            queue.add(message);
        }

        @Override
        public void onPSubscribe(byte[] topic, int totalSubscribed) {
            logger.info("subscribed to pattern [{}], totalSubscribed={}",
                    new String(topic), totalSubscribed);
        }

        @Override
        public void onSubscribe(byte[] topic, int totalSubscribed) {
            logger.info("subscribed to topic [{}], totalSubscribed={}",
                    new String(topic), totalSubscribed);
        }

        @Override
        public void onPUnsubscribe(byte[] topic, int totalSubscribed) {
            logger.info("unsubscribed from pattern [{}], totalSubscribed={}",
                    new String(topic), totalSubscribed);
        }

        @Override
        public void onUnsubscribe(byte[] topic, int totalSubscribed) {
            logger.info("unsubscribed from topic [{}], totalSubscribed={}",
                    new String(topic), totalSubscribed);
        }
    }

    private class Consumer implements Runnable {
        @Override
        public void run() {
            logger.info("consumer thread starting...");
            while (true) {
                if (closed) {
                    break;
                }

                if (queue.isEmpty()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                    continue;
                }

                List<byte[]> todo = new ArrayList<byte[]>();
                queue.drainTo(todo, bulkSize);

                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                final Map<String, Boolean> isReplay = new HashMap<String, Boolean>();

                for (byte[] message : todo) {
                    try {
                        Map<String, Object> inbound = (Map<String, Object>) parseSmileLzf(message);

                        String action = (String) inbound.get("action");
                        String id = (String) inbound.get("id");
                        String parentId = (String) inbound.get("parent");

                        String type = (String) inbound.get("kind");
                        Long version = Long.parseLong((String) inbound
                                .get("version"));

                        isReplay.put(id, (Boolean) inbound.get("replay"));

                        Map<String, Object> theValue = (Map<String, Object>) inbound
                                .get("curr");

                        if ("create".equals(action) || "update".equals(action)) {
                            IndexRequestBuilder theReq = new IndexRequestBuilder(
                                    client);
                            theReq.setIndex(indexName);
                            theReq.setId(id);
                            theReq.setType(type);
                            theReq.setSource(theValue);

                            if (parentId != null) {
                                theReq.setParent(parentId);
                            }

                            theReq.setVersionType(VersionType.EXTERNAL);
                            theReq.setVersion(version);

                            bulkRequestBuilder.add(theReq);
                        } else if ("delete".equals(action)) {
                            bulkRequestBuilder.add(new DeleteRequest(indexName,
                                    type, id));
                        } else {
                            throw new IllegalArgumentException(
                                    inbound.toString());
                        }
                    } catch (Exception e) {
                        logger.warn("failed to parse request", e);
                        continue;
                    }
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("executing bulk with [{}] actions",
                            bulkRequestBuilder.numberOfActions());
                }

                bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        if (response.hasFailures()) {
                            for (BulkItemResponse item : response.items()) {
                                String id = item.getId();

                                if (isReplay.containsKey(id)
                                        && isReplay.get(id)) {
                                    logger.trace("replay error: "
                                            + item.getFailureMessage());
                                } else {
                                    logger.info("execution error {}",
                                            item.getFailureMessage());
                                }
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.warn("failed to execute bulk", e);
                    }
                });

                logger.info("indexer updated {} documents ({} replay)",
                        bulkRequestBuilder.numberOfActions(), isReplay.size());
            }
            logger.info("consumer thread exiting...");
        }
    }

    private class Replay implements Runnable {
        @Override
        public void run() {
            logger.info("replay thread starting...");

            while (true) {
                if (closed) {
                    logger.info("river closed - exiting...");
                    return;
                }

                doReplayPost();

                try {
                    Thread.sleep(replayTimeoutSecs * 1000);
                } catch (InterruptedException e) {
                    break;
                }
            }

            logger.info("replay thread exiting...");
        }
    }

    private void doReplayPost() {
        BufferedReader reader = null;
        try {
            DateTime fromTime = (new DateTime()).minusSeconds(
                    replayTimeoutSecs * 3).withZone(DateTimeZone.UTC);
            DateTime toTime = (new DateTime()).plusHours(1).withZone(
                    DateTimeZone.UTC);

            StringBuilder urlString = new StringBuilder();
            urlString.append(replayUrl);
            urlString.append("?from=");
            urlString.append(format.print(fromTime));
            urlString.append("&to=");
            urlString.append(format.print(toTime));

            String theUrl = urlString.toString();

            URL url = new URL(theUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            reader = new BufferedReader(new InputStreamReader(
                    conn.getInputStream()));
            for (String line = reader.readLine(); line != null; line = reader
                    .readLine()) {
                // throw it away
            }

            logger.info("replayed: {}", theUrl);
        } catch (Exception e) {
            logger.warn("replay error {}", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.warn("replay reader io error {}", e);
                }
            }
        }
    }

    private static Object parseSmileLzf(byte[] valueBytesLzf) {
        try {
            byte[] valueBytes = compressCodec.decode(valueBytesLzf);
            ByteArrayInputStream in = new ByteArrayInputStream(valueBytes);
            SmileParser smile = smileFactory.createJsonParser(in);

            return mapper.readValue(smile, LinkedHashMap.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
