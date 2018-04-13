/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate;

import static org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.NodeStateBackedSegmentStore.newSegmentStore;
import static org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.Streams.asStream;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.proc.Proc;
import org.apache.jackrabbit.oak.tooling.filestore.api.JournalEntry;
import org.apache.jackrabbit.oak.tooling.filestore.api.SegmentStore;
import org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.RecordChangesDiff.Change;
import org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.RecordChangesDiff.ChangeType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.storage.StorageException;

import one.util.streamex.StreamEx;

public class ContentGrowthQueries {

    private static ReadOnlyFileStore fileStore;

    private static SegmentStore segmentStore;

    @BeforeClass
    public static void setup() throws IOException, InvalidFileStoreVersionException, URISyntaxException, InvalidKeyException, StorageException {
        FileStoreBuilder builder = FileStoreUtil.getFileStoreBuilder();
        fileStore = builder.buildReadOnly();
        segmentStore = newSegmentStore(Proc.of(builder.buildProcBackend(fileStore)));
    }

    @AfterClass
    public static void tearDown() {
        segmentStore = null;
        if (fileStore != null) {
            fileStore.close();
        }
    }
    
    @Test
    public void listBinariesAdddedBetweenTwoRevisionsBiggerThanThreshold() {
        String path = "content";
        String rev1 = "a0cf52ea-5858-427d-ab63-7a623fc88180:1886";
        String rev2 = "5c6bc5e5-7196-499e-acad-0668b04bf612:45";
        Stream<Change> changes = StreamEx.of(asStream(segmentStore.journalEntries())
                .filter(j -> (j.segmentId() + ":" + j.recordNumber()).equals(rev1)
                        || (j.segmentId() + ":" + j.recordNumber()).equals(rev2))
                .map(JournalEntry::getRoot)
                .map(node -> node.getChildNode("root").getChildNode(path)))
                .pairMap((a, b) -> {
                    RecordChangesDiff diff = new RecordChangesDiff(path, new ArrayList<>());
                    a.compareAgainstBaseState(b, diff);
                    return diff;
                })
                .flatMap(asStream(RecordChangesDiff::changes))
                .filter(c -> c.type() == ChangeType.PROPERTY_ADDED)
                .filter(c -> c.size() > 300_000);
        
        changes.forEach(System.out::println);
    }
}
