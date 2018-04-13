/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate;

import static com.google.common.collect.Iterables.transform;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.tooling.filestore.bindings.nodestate.Streams.asStream;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;

public class RecordChangesDiff implements NodeStateDiff {
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

        public ChangeType type() {
            return type;
        }
        
        public long size() {
            return size;
        }

        @Override
        public String toString() {
            return "Change [path=" + path + ", type=" + type + ", content=" + content + ", size=" + size + "]";
        }
    }

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
        changes.add(new Change(path, ChangeType.PROPERTY_ADDED, content, getPropertySize(after)));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        StringBuilder content = new StringBuilder();
        content.append("    ^ " + before.getName() + "\n");
        content.append("      - " + toString(before) + "\n");
        content.append("      + " + toString(after));
        changes.add(new Change(path, ChangeType.PROPERTY_CHANGED, content, getPropertySize(after) - getPropertySize(before)));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        StringBuilder content = new StringBuilder();
        content.append("    - " + toString(before));
        changes.add(new Change(path, ChangeType.PROPERTY_DELETED, content, -getPropertySize(before)));
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        String p = concat(path, name);
        StringBuilder content = new StringBuilder();
        content.append("+ " + p);
        changes.add(new Change(p, ChangeType.CHILD_NODE_ADDED, content, getNodeSize(after)));
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
        changes.add(new Change(p, ChangeType.CHILD_NODE_CHANGED, content, getNodeSize(after) - getNodeSize(before)));
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
        changes.add(new Change(p, ChangeType.CHILD_NODE_DELETED, content, -getNodeSize(before)));
        RecordChangesDiff diff = new RecordChangesDiff(p, new ArrayList<>());
        boolean val = MISSING_NODE.compareAgainstBaseState(before, diff);
        diff.changes().forEach(c -> changes.add(c));
        return val;
    }

    public Iterable<Change> changes() {
        return changes;
    }

    private static long getNodeSize(NodeState n) {
        return asStream(n.getProperties()).mapToLong(p -> getPropertySize(p)).sum();
    }
    
    private static long getPropertySize(PropertyState p) {
        long size = 0;

        if (p.getType() == BINARY || p.getType() == BINARIES) {
            for (int i = 0; i < p.count(); i++) {
                size += p.getValue(BINARY, i).length();
            }
        } else {
            for (int i = 0; i < p.count(); i++) {
                size += p.size(i);
            }
        }

        return size;
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
