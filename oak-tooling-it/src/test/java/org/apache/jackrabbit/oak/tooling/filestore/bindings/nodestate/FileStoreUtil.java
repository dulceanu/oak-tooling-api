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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.AssumptionViolatedException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public final class FileStoreUtil {

    private final static String SEGMENT_DIR = System.getProperty("segmentstore", null);
    
    private final static String DATASTORE_DIR = System.getProperty("datastore", null);

    private final static String AZURE_CONNECTION_STRING = System.getProperty("azure_conn_string", null);

    private final static String AZURE_CONTAINER_NAME = System.getProperty("azure_container", null);

    private final static String AZURE_PATH = System.getProperty("azure_path", null);

    private FileStoreUtil() {
    }

    @Nonnull
    public static FileStoreBuilder getFileStoreBuilder() throws URISyntaxException, InvalidKeyException, StorageException, IOException {
        if (SEGMENT_DIR != null) {
            FileStoreBuilder fsb = fileStoreBuilder(new File(SEGMENT_DIR));
            
            if (DATASTORE_DIR != null) {
                fsb.withBlobStore(newBlobStore(new File(DATASTORE_DIR)));
            }
            
            return fsb;
        } else if (AZURE_CONNECTION_STRING != null && AZURE_CONTAINER_NAME != null && AZURE_PATH != null) {
            CloudStorageAccount cloud = CloudStorageAccount.parse(AZURE_CONNECTION_STRING);
            CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(AZURE_CONTAINER_NAME);
            AzurePersistence azurePersistence = new AzurePersistence(container.getDirectoryReference(AZURE_PATH));
            return FileStoreBuilder.fileStoreBuilder(new File(".")).withCustomPersistence(azurePersistence);
        } else {
            throw new AssumptionViolatedException(
                    "No segment store directory specified. " +
                    "Use -Dsegmentstore=/path/to/segmentstore or " +
                    "configure Azure with azure_conn_string, azure_container and azure_path.");
        }
    }
    
    private static BlobStore newBlobStore(File directory) {
        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(directory.getAbsolutePath());
        delegate.init(null);
        return new DataStoreBlobStore(delegate);
    }
}
