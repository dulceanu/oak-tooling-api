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

import static com.google.common.collect.Iterables.transform;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.tooling.filestore.api.Record.Type.NODE;
import static org.apache.jackrabbit.oak.tooling.filestore.api.Segment.Type.DATA;
import static org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.NodeStateBackedSegmentStore.newSegmentStore;
import static org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.Streams.asStream;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.proc.Proc;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.tooling.filestore.api.JournalEntry;
import org.apache.jackrabbit.oak.tooling.filestore.api.Record;
import org.apache.jackrabbit.oak.tooling.filestore.api.Segment;
import org.apache.jackrabbit.oak.tooling.filestore.api.SegmentStore;
import org.apache.jackrabbit.oak.tooling.filestore.api.Tar;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.microsoft.azure.storage.StorageException;

import one.util.streamex.StreamEx;

public class ExampleQueries {

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
    public void listTars() {
        segmentStore.tars()
            .forEach(tar -> System.out.println(tar.name() + " " + tar.size()));
    }

    @Test
    public void tarSize() {
        long tarSizeSum = asStream(segmentStore.tars())
            .filter(tar -> tar.name().endsWith("tar"))
            .mapToLong(Tar::size)
            .sum();

        System.out.println(tarSizeSum);
    }

    @Test
    public void segmentSize() {
        long segmentSizeSum = asStream(segmentStore.tars())
                .filter(tar -> tar.name().endsWith("tar"))
                .flatMap(asStream(Tar::segments))
                .mapToLong(Segment::length)
                .sum();

        System.out.println(segmentSizeSum);
    }

    @Test
    public void referenceCount() {
        long referenceCount = asStream(segmentStore.tars())
                .filter(tar -> tar.name().endsWith("tar"))
                .flatMap(asStream(Tar::segments))
                .filter(Segment.isOfType(DATA))
                .flatMap(asStream(Segment::references))
                .count();

        System.out.println(referenceCount);
    }

    @Test
    public void checkpointCountPerRevision() {
        Stream<Long> checkpointCountPerRevision = asStream(segmentStore.journalEntries())
                .map(JournalEntry::getRoot)
                .map(n -> n.getChildNode("checkpoints").getChildNodeCount(Integer.MAX_VALUE));

        List<Long> latest100Checkpoints = checkpointCountPerRevision
                .limit(100)
                .collect(Collectors.toList());
        System.out.println(latest100Checkpoints);
    }

    @Test
    public void nodesInRecords() {
        Stream<NodeState> nodes = asStream(segmentStore.tars())
                .flatMap(asStream(Tar::segments))
                .flatMap(asStream(Segment::records))
                .filter(Record.isOfType(NODE))
                .limit(10)
                .map(Record::root)
                .map(Optional::get);

        nodes.forEach(System.out::println);
    }

    static enum ChangeType {
    	PROPERTY_ADDED,
    	PROPERTY_CHANGED,
    	PROPERTY_DELETED,
    	CHILD_NODE_ADDED,
    	CHILD_NODE_CHANGED,
    	CHILD_NODE_DELETED
    }
    
    static class Change {
    	private String path;
    	private ChangeType type;
    	private StringBuilder content;
    	private long size;
    	
    	public Change(String path, ChangeType type, StringBuilder content, long size) {
    		this.path = path;
    		this.type = type;
    		this.content = content;
    		this.size = size;
    	}

		@Override
		public String toString() {
			return "Change [path=" + path + ", type=" + type + ", content=" + content + ", size=" + size + "]";
		}
    }
    
    static class RecordChangesDiff implements NodeStateDiff {
		private static final Function<Blob, String> BLOB_LENGTH = new Function<Blob, String>() {

			@Override
			public String apply(Blob b) {
				return safeGetLength(b);
			}

			private String safeGetLength(Blob b) {
				try {
					return byteCountToDisplaySize(b.length());
				} catch (IllegalStateException e) {
					// missing BlobStore probably
				}
				return "[N/A]";
			}

		};
		
    	private final List<Change> changes;
    	private final String path;
    	
    	public RecordChangesDiff(String path, List<Change> changes) {
    		this.path = path;
    		this.changes = changes;
    	}
    	
		@Override
		public boolean propertyAdded(PropertyState after) {
			StringBuilder content = new StringBuilder();
			content.append("    + " + toString(after));
			changes.add(new Change(path, ChangeType.PROPERTY_ADDED, content, 0));
			return true;
		}

		@Override
		public boolean propertyChanged(PropertyState before, PropertyState after) {
			StringBuilder content = new StringBuilder();
			content.append("    ^ " + before.getName() + "\n");
            content.append("      - " + toString(before) + "\n");
            content.append("      + " + toString(after));
            
            changes.add(new Change(path, ChangeType.PROPERTY_CHANGED, content, 0));
            
			return true;
		}

		@Override
		public boolean propertyDeleted(PropertyState before) {
			StringBuilder content = new StringBuilder();
			content.append("    - " + toString(before));
			changes.add(new Change(path, ChangeType.PROPERTY_DELETED, content, 0));
			return true;
		}

		@Override
		public boolean childNodeAdded(String name, NodeState after) {
			String p = concat(path, name);
			StringBuilder content = new StringBuilder();
			content.append("+ " + p);
			changes.add(new Change(p, ChangeType.CHILD_NODE_ADDED, content, 0));
			RecordChangesDiff diff = new RecordChangesDiff(p, new ArrayList<>());
			boolean val = after.compareAgainstBaseState(EMPTY_NODE, diff);
			diff.changes().forEach(c -> changes.add(c));
			return val;
	    }

		@Override
		public boolean childNodeChanged(String name, NodeState before, NodeState after) {
	        String p = concat(path, name);
			StringBuilder content = new StringBuilder();
			content.append("^ " + p);
			changes.add(new Change(p, ChangeType.CHILD_NODE_CHANGED, content, 0));
			RecordChangesDiff diff = new RecordChangesDiff(p, new ArrayList<>());
			boolean val = after.compareAgainstBaseState(before, diff);
			diff.changes().forEach(c -> changes.add(c));
			return val;
		}

		@Override
		public boolean childNodeDeleted(String name, NodeState before) {
			String p = concat(path, name);
			StringBuilder content = new StringBuilder();
			content.append("- " + p);
			changes.add(new Change(p, ChangeType.CHILD_NODE_CHANGED, content, 0));
			RecordChangesDiff diff = new RecordChangesDiff(p, new ArrayList<>());
			boolean val = MISSING_NODE.compareAgainstBaseState(before, diff);
			diff.changes().forEach(c -> changes.add(c));
			return val;
		}
		
		public Iterable<Change> changes() {
			return changes;
		}
		
		private static String toString(PropertyState ps) {
	        StringBuilder val = new StringBuilder();
	        val.append(ps.getName()).append("<").append(ps.getType()).append(">");
	        if (ps.getType() == BINARY) {
	            String v = BLOB_LENGTH.apply(ps.getValue(BINARY));
	            val.append(" = {").append(v).append("}");
	        } else if (ps.getType() == BINARIES) {
	            String v = transform(ps.getValue(BINARIES), BLOB_LENGTH).toString();
	            val.append("[").append(ps.count()).append("] = ").append(v);
	        } else if (ps.isArray()) {
	            val.append("[").append(ps.count()).append("] = ").append(ps.getValue(STRINGS));
	        } else {
	            val.append(" = ").append(ps.getValue(STRING));
	        }
	        return ps.getName() + "<" + ps.getType() + ">" + val.toString();
	    }
    }
    
    @Test
	public void analyzeContentGrowth() {
		String path = "content";
		String rev1 = "a0cf52ea-5858-427d-ab63-7a623fc88180:1886";
		String rev2 = "3e22c18c-d29f-4348-aad5-8a47678ee66c:2415";
		Stream<Change> changes = StreamEx.of(asStream(segmentStore.journalEntries())
				.filter(j -> (j.segmentId()+":"+j.recordNumber()).equals(rev1)
							|| (j.segmentId()+":"+j.recordNumber()).equals(rev2))
				.map(JournalEntry::getRoot)
				.map(node -> node.getChildNode("root").getChildNode(path)))
				.pairMap((a,b) -> {
					RecordChangesDiff diff = new RecordChangesDiff(path, new ArrayList<>());
					a.compareAgainstBaseState(b, diff);
					return diff;
				})
				.flatMap(asStream(RecordChangesDiff::changes))
				.filter(c -> c.type == ChangeType.PROPERTY_ADDED);
		changes.forEach(System.out::println);
//		System.out.println("Changes:"  + changes.count());
	}
}
