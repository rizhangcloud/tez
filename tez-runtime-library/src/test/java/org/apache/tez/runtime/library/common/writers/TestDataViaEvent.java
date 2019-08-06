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
package org.apache.tez.runtime.library.common.writers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.*;
import org.apache.tez.runtime.api.events.*;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleInputEventHandlerImpl;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter.SpillInfo;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.apache.tez.runtime.library.utils.DATA_RANGE_IN_MB;
import org.junit.*;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.runtime.library.utils.DATA_RANGE_IN_MB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;


@RunWith(value = Parameterized.class)
public class TestDataViaEvent {

    private static final Logger LOG = LoggerFactory.getLogger(TestUnorderedPartitionedKVWriter.class);

    private static final String HOST_STRING = "localhost";
    private static final int SHUFFLE_PORT = 4000;

    private static String testTmpDir = System.getProperty("test.build.data", "/tmp");
    private static final Path TEST_ROOT_DIR = new Path(testTmpDir,
            TestUnorderedPartitionedKVWriter.class.getSimpleName());
    private static FileSystem localFs;

    private boolean shouldCompress;
    private TezRuntimeConfiguration.ReportPartitionStats reportPartitionStats;
    private Configuration defaultConf = new Configuration();

    private static final String PATH_COMPONENT = "attempttmp";

    public TestDataViaEvent(boolean shouldCompress,
                                            TezRuntimeConfiguration.ReportPartitionStats reportPartitionStats) {
        this.shouldCompress = shouldCompress;
        this.reportPartitionStats = reportPartitionStats;
    }

    @SuppressWarnings("deprecation")
    @Parameterized.Parameters(name = "test[{0}, {1}]")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
                { false, TezRuntimeConfiguration.ReportPartitionStats.DISABLED },
                { false, TezRuntimeConfiguration.ReportPartitionStats.ENABLED },
                { false, TezRuntimeConfiguration.ReportPartitionStats.NONE },
                { false, TezRuntimeConfiguration.ReportPartitionStats.MEMORY_OPTIMIZED },
                { false, TezRuntimeConfiguration.ReportPartitionStats.PRECISE },
                { true, TezRuntimeConfiguration.ReportPartitionStats.DISABLED },
                { true, TezRuntimeConfiguration.ReportPartitionStats.ENABLED },
                { true, TezRuntimeConfiguration.ReportPartitionStats.NONE },
                { true, TezRuntimeConfiguration.ReportPartitionStats.MEMORY_OPTIMIZED },
                { true, TezRuntimeConfiguration.ReportPartitionStats.PRECISE }};
        return Arrays.asList(data);
    }

    @Before
    public void setup() throws IOException {
        LOG.info("Setup. Using test dir: " + TEST_ROOT_DIR);
        localFs = FileSystem.getLocal(new Configuration());
        localFs.delete(TEST_ROOT_DIR, true);
        localFs.mkdirs(TEST_ROOT_DIR);
    }

    @After
    public void cleanup() throws IOException {
        LOG.info("CleanUp");
        localFs.delete(TEST_ROOT_DIR, true);
    }


    @Test(timeout = 1000000)
    public void testNoSpill_SinglePartition() throws IOException, InterruptedException {
        baseTest(100, 1, null, shouldCompress, -1, 0);
    }


    private static String createRandomString(int size) {
        StringBuilder sb = new StringBuilder(size);
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int r = Math.abs(random.nextInt()) % 26;
            sb.append((char) (65 + r));
        }
        return sb.toString();
    }

    private int[] getPartitionStats(VertexManagerEvent vme) throws IOException {
        RoaringBitmap partitionStats = new RoaringBitmap();
        ShuffleUserPayloads.VertexManagerEventPayloadProto
                payload = ShuffleUserPayloads.VertexManagerEventPayloadProto
                .parseFrom(ByteString.copyFrom(vme.getUserPayload()));
        if (!reportPartitionStats.isEnabled()) {
            assertFalse(payload.hasPartitionStats());
            assertFalse(payload.hasDetailedPartitionStats());
            return null;
        }
        if (reportPartitionStats.isPrecise()) {
            assertTrue(payload.hasDetailedPartitionStats());
            List<Integer> sizeInMBList =
                    payload.getDetailedPartitionStats().getSizeInMbList();
            int[] stats = new int[sizeInMBList.size()];
            for (int i=0; i<sizeInMBList.size(); i++) {
                stats[i] += sizeInMBList.get(i);
            }
            return stats;
        } else {
            assertTrue(payload.hasPartitionStats());
            ByteString compressedPartitionStats = payload.getPartitionStats();
            byte[] rawData = TezCommonUtils.decompressByteStringToByteArray(
                    compressedPartitionStats);
            ByteArrayInputStream bin = new ByteArrayInputStream(rawData);
            partitionStats.deserialize(new DataInputStream(bin));
            int[] stats = new int[partitionStats.getCardinality()];
            Iterator<Integer> it = partitionStats.iterator();
            final DATA_RANGE_IN_MB[] RANGES = DATA_RANGE_IN_MB.values();
            final int RANGE_LEN = RANGES.length;
            while (it.hasNext()) {
                int pos = it.next();
                int index = ((pos) / RANGE_LEN);
                int rangeIndex = ((pos) % RANGE_LEN);
                if (RANGES[rangeIndex].getSizeInMB() > 0) {
                    stats[index] += RANGES[rangeIndex].getSizeInMB();
                }
            }
            return stats;
        }
    }

    private void verifyPartitionStats(VertexManagerEvent vme,
                                      BitSet expectedPartitionsWithData) throws IOException {
        int[] stats = getPartitionStats(vme);
        if (stats == null) {
            return;
        }
        for (int i = 0; i < stats.length; i++) {
            // The stats should be greater than zero if and only if
            // the partition has data
            assertTrue(expectedPartitionsWithData.get(i) == (stats[i] > 0));
        }
    }


    private void baseTest(int numRecords, int numPartitions, Set<Integer> skippedPartitions,
                          boolean shouldCompress, int maxSingleBufferSizeBytes, int bufferMergePercent)
            throws IOException, InterruptedException {
        baseTest(numRecords, numPartitions, skippedPartitions, shouldCompress,
                maxSingleBufferSizeBytes, bufferMergePercent, 2048);
    }

    private void baseTest(int numRecords, int numPartitions, Set<Integer> skippedPartitions,
                          boolean shouldCompress, int maxSingleBufferSizeBytes, int bufferMergePercent, int
                                  availableMemory)
            throws IOException, InterruptedException {
        PartitionerForTest partitioner = new PartitionerForTest();
        ApplicationId appId = ApplicationId.newInstance(10000000, 1);
        TezCounters counters = new TezCounters();
        String uniqueId = UUID.randomUUID().toString();
        int dagId = 1;
        String auxiliaryService = defaultConf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
        OutputContext outputContext = createMockOutputContext(counters, appId, uniqueId, auxiliaryService);

        Configuration conf = createConfiguration(outputContext, IntWritable.class, LongWritable.class,
                shouldCompress, maxSingleBufferSizeBytes);
        conf.setInt(
                TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT,
                bufferMergePercent);

        /* start declarations for test DataViaEvent */

        InputContext inputContext = mock(InputContext.class);
        ShuffleManager shuffleManager = mock(ShuffleManager.class);
        FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

        ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(inputContext,
                shuffleManager, inputAllocator, null, false, 0, false);
        int taskIndex = 1;
        /* end declarations for test DataViaEvent */

        CompressionCodec codec = null;
        if (shouldCompress) {
            codec = new DefaultCodec();
            ((Configurable) codec).setConf(conf);
        }

        int numOutputs = numPartitions;
        int numRecordsWritten = 0;

        Map<Integer, Multimap<Integer, Long>> expectedValues = new HashMap<Integer, Multimap<Integer, Long>>();
        for (int i = 0; i < numOutputs; i++) {
            expectedValues.put(i, LinkedListMultimap.<Integer, Long> create());
        }

        UnorderedPartitionedKVWriter kvWriter = new UnorderedPartitionedKVWriterForTest(outputContext,
                conf, numOutputs, availableMemory);

        int sizePerBuffer = kvWriter.sizePerBuffer;
        int sizePerRecord = 4 + 8; // IntW + LongW
        int sizePerRecordWithOverhead = sizePerRecord + 12; // Record + META_OVERHEAD

        IntWritable intWritable = new IntWritable();
        LongWritable longWritable = new LongWritable();
        BitSet partitionsWithData = new BitSet(numPartitions);
        for (int i = 0; i < numRecords; i++) {
            intWritable.set(i);
            longWritable.set(i);
            int partition = partitioner.getPartition(intWritable, longWritable, numOutputs);
            if (skippedPartitions != null && skippedPartitions.contains(partition)) {
                continue;
            }
            partitionsWithData.set(partition);
            expectedValues.get(partition).put(intWritable.get(), longWritable.get());
            kvWriter.write(intWritable, longWritable);
            numRecordsWritten++;
        }
        List<Event> events = kvWriter.close();

        if (numPartitions == 1) {
            assertEquals(true, kvWriter.skipBuffers);
        }

        int recordsPerBuffer = sizePerBuffer / sizePerRecordWithOverhead;
        int numExpectedSpills = numRecordsWritten / recordsPerBuffer / kvWriter.spillLimit;

        verify(outputContext, never()).reportFailure(any(TaskFailureType.class), any(Throwable.class), any(String.class));

        assertNull(kvWriter.currentBuffer);
        assertEquals(0, kvWriter.availableBuffers.size());

        // Verify the counters
        TezCounter outputRecordBytesCounter = counters.findCounter(TaskCounter.OUTPUT_BYTES);
        TezCounter outputRecordsCounter = counters.findCounter(TaskCounter.OUTPUT_RECORDS);
        TezCounter outputBytesWithOverheadCounter = counters
                .findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
        TezCounter fileOutputBytesCounter = counters.findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
        TezCounter spilledRecordsCounter = counters.findCounter(TaskCounter.SPILLED_RECORDS);
        TezCounter additionalSpillBytesWritternCounter = counters
                .findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
        TezCounter additionalSpillBytesReadCounter = counters
                .findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
        TezCounter numAdditionalSpillsCounter = counters
                .findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
        assertEquals(numRecordsWritten * sizePerRecord, outputRecordBytesCounter.getValue());
        if (numPartitions > 1) {
            assertEquals(numRecordsWritten * sizePerRecordWithOverhead, outputBytesWithOverheadCounter.getValue());
        }
        assertEquals(numRecordsWritten, outputRecordsCounter.getValue());

        /*
        long fileOutputBytes = fileOutputBytesCounter.getValue();
        if (numRecordsWritten > 0) {
            assertTrue(fileOutputBytes > 0);
            if (!shouldCompress) {
                assertTrue(fileOutputBytes > outputRecordBytesCounter.getValue());
            }
        } else {
            assertEquals(0, fileOutputBytes);
        }
        assertEquals(recordsPerBuffer * numExpectedSpills, spilledRecordsCounter.getValue());
        long additionalSpillBytesWritten = additionalSpillBytesWritternCounter.getValue();
        long additionalSpillBytesRead = additionalSpillBytesReadCounter.getValue();
        if (numExpectedSpills == 0) {
            assertEquals(0, additionalSpillBytesWritten);
            assertEquals(0, additionalSpillBytesRead);
        } else {
            assertTrue(additionalSpillBytesWritten > 0);
            assertTrue(additionalSpillBytesRead > 0);
            if (!shouldCompress) {
                assertTrue(additionalSpillBytesWritten > (recordsPerBuffer * numExpectedSpills * sizePerRecord));
                assertTrue(additionalSpillBytesRead > (recordsPerBuffer * numExpectedSpills * sizePerRecord));
            }
        }
        assertEquals(additionalSpillBytesWritten, additionalSpillBytesRead);
        // due to multiple threads, buffers could be merged in chunks in scheduleSpill.
        assertTrue(numExpectedSpills >= numAdditionalSpillsCounter.getValue());
         */

        BitSet emptyPartitionBits = null;
        // Verify the events returned
        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof VertexManagerEvent);
        VertexManagerEvent vme = (VertexManagerEvent) events.get(0);
        verifyPartitionStats(vme, partitionsWithData);
        assertTrue(events.get(1) instanceof CompositeDataMovementEvent);

        CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(1);
        assertEquals(0, cdme.getSourceIndexStart());
        assertEquals(numOutputs, cdme.getCount());


        ShuffleUserPayloads.DataMovementEventPayloadProto eventProto =
                ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(
                        cdme.getUserPayload()));

        if (eventProto.hasEmptyPartitions()) {
            byte[] emptyPartitionsBytesString =
                    TezCommonUtils.decompressByteStringToByteArray(
                            eventProto.getEmptyPartitions());
            BitSet emptyPartitionBitSet = TezUtilsInternal.fromByteArray(emptyPartitionsBytesString);
        }

        /* start verify event handler */
        for (int i = 0; i < cdme.getCount(); i++) {
            handler.handleEvents(Collections.singletonList(cdme.getEvents().iterator().next()));

            //CompositeInputAttemptIdentifier expectedIdentifier = new CompositeInputAttemptIdentifier(taskIndex, 0,
                    // PATH_COMPONENT, 1);
            //verify(shuffleManager).addKnownInput(eq(HOST_STRING), eq(SHUFFLE_PORT), eq(expectedIdentifier), eq(0));
        }
        /* end verify event handler */


        if (skippedPartitions == null && numRecordsWritten > 0) {
            assertFalse(eventProto.hasEmptyPartitions());
            emptyPartitionBits = new BitSet(numPartitions);
        } else {
            assertTrue(eventProto.hasEmptyPartitions());
            byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(eventProto
                    .getEmptyPartitions());
            emptyPartitionBits = TezUtilsInternal.fromByteArray(emptyPartitions);
            if (numRecordsWritten == 0) {
                assertEquals(numPartitions, emptyPartitionBits.cardinality());
            } else {
                for (Integer e : skippedPartitions) {
                    assertTrue(emptyPartitionBits.get(e));
                }
                assertEquals(skippedPartitions.size(), emptyPartitionBits.cardinality());
            }
        }
        if (emptyPartitionBits.cardinality() != numPartitions) {
            assertEquals(HOST_STRING, eventProto.getHost());
            assertEquals(SHUFFLE_PORT, eventProto.getPort());
            assertEquals(uniqueId, eventProto.getPathComponent());
        } else {
            assertFalse(eventProto.hasHost());
            assertFalse(eventProto.hasPort());
            assertFalse(eventProto.hasPathComponent());
        }

        // Verify the actual data
        TezTaskOutput taskOutput = new TezTaskOutputFiles(conf, uniqueId, dagId);
        Path outputFilePath = kvWriter.finalOutPath;
        Path spillFilePath = kvWriter.finalIndexPath;

        if (numRecordsWritten <= 0) {
            return;
        }


        boolean isInMem =false;
        isInMem= eventProto.hasData();

        assertTrue(localFs.exists(outputFilePath)||isInMem);
        assertTrue(localFs.exists(spillFilePath)||isInMem);
        //Todo: change the assertion
    /*
    assertEquals("Incorrect output permissions", (short)0640,
        localFs.getFileStatus(outputFilePath).getPermission().toShort());
    assertEquals("Incorrect index permissions", (short)0640,
        localFs.getFileStatus(spillFilePath).getPermission().toShort());
     */
        // verify no intermediate spill files have been left around
        synchronized (kvWriter.spillInfoList) {
            for (UnorderedPartitionedKVWriter.SpillInfo spill : kvWriter.spillInfoList) {
                assertFalse("lingering intermediate spill file " + spill.outPath,
                        localFs.exists(spill.outPath));
            }
        }

        // Special case for 0 records.
        TezSpillRecord spillRecord = new TezSpillRecord(spillFilePath, conf);
        DataInputBuffer keyBuffer = new DataInputBuffer();
        DataInputBuffer valBuffer = new DataInputBuffer();
        IntWritable keyDeser = new IntWritable();
        LongWritable valDeser = new LongWritable();
        for (int i = 0; i < numOutputs; i++) {
            TezIndexRecord indexRecord = spillRecord.getIndex(i);
            if (skippedPartitions != null && skippedPartitions.contains(i)) {
                assertFalse("The Index Record for partition " + i + " should not have any data", indexRecord.hasData());
                continue;
            }
            InputStream inStream;
            if (isInMem) {
                inStream = new ByteArrayInputStream(eventProto.getData().toByteArray());
            } else
            {
                FSDataInputStream  tmpStream = FileSystem.getLocal(conf).open(outputFilePath);
                tmpStream.seek(indexRecord.getStartOffset());
                inStream = tmpStream;
            }
            IFile.Reader reader = new IFile.Reader(inStream, indexRecord.getPartLength(), codec, null,
                    null, false, 0, -1);

            while (reader.nextRawKey(keyBuffer)) {
                reader.nextRawValue(valBuffer);
                keyDeser.readFields(keyBuffer);
                valDeser.readFields(valBuffer);
                int partition = partitioner.getPartition(keyDeser, valDeser, numOutputs);
                assertTrue(expectedValues.get(partition).remove(keyDeser.get(), valDeser.get()));
            }
            inStream.close();
        }
        for (int i = 0; i < numOutputs; i++) {
            assertEquals(0, expectedValues.get(i).size());
            expectedValues.remove(i);
        }
        assertEquals(0, expectedValues.size());
        verify(outputContext, atLeast(1)).notifyProgress();
    }

    private OutputContext createMockOutputContext(TezCounters counters, ApplicationId appId,
                                                  String uniqueId, String auxiliaryService) {
        OutputContext outputContext = mock(OutputContext.class);
        doReturn(counters).when(outputContext).getCounters();
        doReturn(appId).when(outputContext).getApplicationId();
        doReturn(1).when(outputContext).getDAGAttemptNumber();
        doReturn("dagName").when(outputContext).getDAGName();
        doReturn("destinationVertexName").when(outputContext).getDestinationVertexName();
        doReturn(1).when(outputContext).getOutputIndex();
        doReturn(1).when(outputContext).getTaskAttemptNumber();
        doReturn(1).when(outputContext).getTaskIndex();
        doReturn(1).when(outputContext).getTaskVertexIndex();
        doReturn("vertexName").when(outputContext).getTaskVertexName();
        doReturn(uniqueId).when(outputContext).getUniqueIdentifier();

        doAnswer(new Answer<ByteBuffer>() {
            @Override
            public ByteBuffer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer portBuffer = ByteBuffer.allocate(4);
                portBuffer.mark();
                portBuffer.putInt(SHUFFLE_PORT);
                portBuffer.reset();
                return portBuffer;
            }
        }).when(outputContext).getServiceProviderMetaData(eq(auxiliaryService));

        Path outDirBase = new Path(TEST_ROOT_DIR, "outDir_" + uniqueId);
        String[] outDirs = new String[] { outDirBase.toString() };
        doReturn(outDirs).when(outputContext).getWorkDirs();
        return outputContext;
    }

    private Configuration createConfiguration(OutputContext outputContext,
                                              Class<? extends Writable> keyClass, Class<? extends Writable> valClass,
                                              boolean shouldCompress, int maxSingleBufferSizeBytes) {
        return createConfiguration(outputContext, keyClass, valClass, shouldCompress,
                maxSingleBufferSizeBytes, PartitionerForTest.class);
    }

    private Configuration createConfiguration(OutputContext outputContext,
                                              Class<? extends Writable> keyClass, Class<? extends Writable> valClass,
                                              boolean shouldCompress, int maxSingleBufferSizeBytes,
                                              Class<? extends Partitioner> partitionerClass) {
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
        conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, outputContext.getWorkDirs());
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClass.getName());
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valClass.getName());
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, partitionerClass.getName());
        if (maxSingleBufferSizeBytes >= 0) {
            conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES,
                    maxSingleBufferSizeBytes);
        }
        conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, shouldCompress);
        if (shouldCompress) {
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC,
                    DefaultCodec.class.getName());
        }
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
                reportPartitionStats.getType());
        return conf;
    }

    public static class PartitionerForTest implements Partitioner {
        @Override
        public int getPartition(Object key, Object value, int numPartitions) {
            if (key instanceof IntWritable) {
                return ((IntWritable) key).get() % numPartitions;
            } else {
                throw new UnsupportedOperationException(
                        "Test partitioner expected to be called with IntWritable only");
            }
        }
    }

    private static class UnorderedPartitionedKVWriterForTest extends UnorderedPartitionedKVWriter {

        public UnorderedPartitionedKVWriterForTest(OutputContext outputContext, Configuration conf,
                                                   int numOutputs, long availableMemoryBytes) throws IOException {
            super(outputContext, conf, numOutputs, availableMemoryBytes);
        }

        @Override
        String getHost() {
            return HOST_STRING;
        }
    }



       /*
    @Test(timeout = 1000000)
    public void testNoSpill() throws IOException, InterruptedException {
        baseTest(10, 10, null, shouldCompress, -1, 0);
    }

    @Test(timeout = 1000000)
    public void testSingleSpill() throws IOException, InterruptedException {
        baseTest(50, 10, null, shouldCompress, -1, 0);
    }


    @Test(timeout = 1000000)
    public void testNoRecords() throws IOException, InterruptedException {
        baseTest(0, 10, null, shouldCompress, -1, 0);
    }

    @Test(timeout = 10000000)
    public void testNoRecords_SinglePartition() throws IOException, InterruptedException {
        // skipBuffers
        baseTest(0, 1, null, shouldCompress, -1, 0);
    }
    */


    public void textTest(int numRegularRecords, int numPartitions, long availableMemory,
                         int numLargeKeys, int numLargevalues, int numLargeKvPairs,
                         boolean pipeliningEnabled, boolean isFinalMergeEnabled) throws IOException,
            InterruptedException {
        Partitioner partitioner = new HashPartitioner();
        ApplicationId appId = ApplicationId.newInstance(10000000, 1);
        TezCounters counters = new TezCounters();
        String uniqueId = UUID.randomUUID().toString();
        int dagId = 1;
        String auxiliaryService = defaultConf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
        OutputContext outputContext = createMockOutputContext(counters, appId, uniqueId, auxiliaryService);
        Random random = new Random();

        Configuration conf = createConfiguration(outputContext, Text.class, Text.class, shouldCompress,
                -1, HashPartitioner.class);
        conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, pipeliningEnabled);
        conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, isFinalMergeEnabled);

        CompressionCodec codec = null;
        if (shouldCompress) {
            codec = new DefaultCodec();
            ((Configurable) codec).setConf(conf);
        }

        int numRecordsWritten = 0;

        Map<Integer, Multimap<String, String>> expectedValues = new HashMap<Integer, Multimap<String, String>>();
        for (int i = 0; i < numPartitions; i++) {
            expectedValues.put(i, LinkedListMultimap.<String, String> create());
        }

        UnorderedPartitionedKVWriter kvWriter = new TestDataViaEvent.UnorderedPartitionedKVWriterForTest(outputContext,
                conf, numPartitions, availableMemory);

        int sizePerBuffer = kvWriter.sizePerBuffer;

        BitSet partitionsWithData = new BitSet(numPartitions);
        Text keyText = new Text();
        Text valText = new Text();
        for (int i = 0; i < numRegularRecords; i++) {
            String key = createRandomString(Math.abs(random.nextInt(10)));
            String val = createRandomString(Math.abs(random.nextInt(20)));
            keyText.set(key);
            valText.set(val);
            int partition = partitioner.getPartition(keyText, valText, numPartitions);
            partitionsWithData.set(partition);
            expectedValues.get(partition).put(key, val);
            kvWriter.write(keyText, valText);
            numRecordsWritten++;
        }

        // Write Large key records
        for (int i = 0; i < numLargeKeys; i++) {
            String key = createRandomString(sizePerBuffer + Math.abs(random.nextInt(100)));
            String val = createRandomString(Math.abs(random.nextInt(20)));
            keyText.set(key);
            valText.set(val);
            int partition = partitioner.getPartition(keyText, valText, numPartitions);
            partitionsWithData.set(partition);
            expectedValues.get(partition).put(key, val);
            kvWriter.write(keyText, valText);
            numRecordsWritten++;
        }
        if (pipeliningEnabled) {
            verify(outputContext, times(numLargeKeys)).sendEvents(anyListOf(Event.class));
        }

        // Write Large val records
        for (int i = 0; i < numLargevalues; i++) {
            String key = createRandomString(Math.abs(random.nextInt(10)));
            String val = createRandomString(sizePerBuffer + Math.abs(random.nextInt(100)));
            keyText.set(key);
            valText.set(val);
            int partition = partitioner.getPartition(keyText, valText, numPartitions);
            partitionsWithData.set(partition);
            expectedValues.get(partition).put(key, val);
            kvWriter.write(keyText, valText);
            numRecordsWritten++;
        }
        if (pipeliningEnabled) {
            verify(outputContext, times(numLargevalues + numLargeKeys)).sendEvents(anyListOf(Event.class));
        }

        // Write records where key + val are large (but both can fit in the buffer individually)
        for (int i = 0; i < numLargeKvPairs; i++) {
            String key = createRandomString(sizePerBuffer / 2 + Math.abs(random.nextInt(100)));
            String val = createRandomString(sizePerBuffer / 2 + Math.abs(random.nextInt(100)));
            keyText.set(key);
            valText.set(val);
            int partition = partitioner.getPartition(keyText, valText, numPartitions);
            partitionsWithData.set(partition);
            expectedValues.get(partition).put(key, val);
            kvWriter.write(keyText, valText);
            numRecordsWritten++;
        }
        if (pipeliningEnabled) {
            verify(outputContext, times(numLargevalues + numLargeKeys + numLargeKvPairs))
                    .sendEvents(anyListOf(Event.class));
        }

        List<Event> events = kvWriter.close();
        verify(outputContext, never()).reportFailure(any(TaskFailureType.class), any(Throwable.class), any(String.class));

        if (!pipeliningEnabled) {
            VertexManagerEvent vmEvent = null;
            for (Event event : events) {
                if (event instanceof VertexManagerEvent) {
                    assertNull(vmEvent);
                    vmEvent = (VertexManagerEvent) event;
                }
            }
            VertexManagerEventPayloadProto vmEventPayload =
                    VertexManagerEventPayloadProto.parseFrom(
                            ByteString.copyFrom(vmEvent.getUserPayload().asReadOnlyBuffer()));
            assertEquals(numRecordsWritten, vmEventPayload.getNumRecord());
        }

        TezCounter outputLargeRecordsCounter = counters.findCounter(TaskCounter.OUTPUT_LARGE_RECORDS);
        assertEquals(numLargeKeys + numLargevalues + numLargeKvPairs,
                outputLargeRecordsCounter.getValue());

        if (pipeliningEnabled || !isFinalMergeEnabled) {
            // verify spill data files and index file exist
            for (int i = 0; i < kvWriter.numSpills.get(); i++) {
                assertTrue(localFs.exists(kvWriter.outputFileHandler.getSpillFileForWrite(i, 0)));
                assertTrue(localFs.exists(kvWriter.outputFileHandler.getSpillIndexFileForWrite(i, 0)));
            }
            return;
        }

        // Validate the events
        assertEquals(2, events.size());

        assertTrue(events.get(0) instanceof VertexManagerEvent);
        VertexManagerEvent vme = (VertexManagerEvent) events.get(0);
        verifyPartitionStats(vme, partitionsWithData);

        assertTrue(events.get(1) instanceof CompositeDataMovementEvent);
        CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(1);
        assertEquals(0, cdme.getSourceIndexStart());
        assertEquals(numPartitions, cdme.getCount());
        DataMovementEventPayloadProto eventProto = DataMovementEventPayloadProto.parseFrom(
                ByteString.copyFrom(cdme.getUserPayload()));
        assertFalse(eventProto.hasData());
        BitSet emptyPartitionBits = null;
        if (partitionsWithData.cardinality() != numPartitions) {
            assertTrue(eventProto.hasEmptyPartitions());
            byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(
                    eventProto.getEmptyPartitions());
            emptyPartitionBits = TezUtilsInternal.fromByteArray(emptyPartitions);
            assertEquals(numPartitions - partitionsWithData.cardinality(),
                    emptyPartitionBits.cardinality());
        } else {
            assertFalse(eventProto.hasEmptyPartitions());
            emptyPartitionBits = new BitSet(numPartitions);
        }
        assertEquals(HOST_STRING, eventProto.getHost());
        assertEquals(SHUFFLE_PORT, eventProto.getPort());
        assertEquals(uniqueId, eventProto.getPathComponent());

        // Verify the data
        // Verify the actual data
        TezTaskOutput taskOutput = new TezTaskOutputFiles(conf, uniqueId, dagId);
        Path outputFilePath = kvWriter.finalOutPath;
        Path spillFilePath = kvWriter.finalIndexPath;
        if (numRecordsWritten > 0) {
            assertTrue(localFs.exists(outputFilePath));
            assertTrue(localFs.exists(spillFilePath));
            assertEquals("Incorrect output permissions", (short)0640,
                    localFs.getFileStatus(outputFilePath).getPermission().toShort());
            assertEquals("Incorrect index permissions", (short)0640,
                    localFs.getFileStatus(spillFilePath).getPermission().toShort());
        } else {
            return;
        }

        // Special case for 0 records.
        TezSpillRecord spillRecord = new TezSpillRecord(spillFilePath, conf);
        DataInputBuffer keyBuffer = new DataInputBuffer();
        DataInputBuffer valBuffer = new DataInputBuffer();
        Text keyDeser = new Text();
        Text valDeser = new Text();
        for (int i = 0; i < numPartitions; i++) {
            if (emptyPartitionBits.get(i)) {
                continue;
            }
            TezIndexRecord indexRecord = spillRecord.getIndex(i);
            FSDataInputStream inStream = FileSystem.getLocal(conf).open(outputFilePath);
            inStream.seek(indexRecord.getStartOffset());
            IFile.Reader reader = new IFile.Reader(inStream, indexRecord.getPartLength(), codec, null,
                    null, false, 0, -1);
            while (reader.nextRawKey(keyBuffer)) {
                reader.nextRawValue(valBuffer);
                keyDeser.readFields(keyBuffer);
                valDeser.readFields(valBuffer);
                int partition = partitioner.getPartition(keyDeser, valDeser, numPartitions);
                assertTrue(expectedValues.get(partition).remove(keyDeser.toString(), valDeser.toString()));
            }
            inStream.close();
        }
        for (int i = 0; i < numPartitions; i++) {
            assertEquals(0, expectedValues.get(i).size());
            expectedValues.remove(i);
        }
        assertEquals(0, expectedValues.size());
    }


}


