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
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.utils.BufferUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.counters.TezCounter;

import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;

/**
 * <code>BufferedDirectOutput</code> extends the <code> Writer</code> inside
 * <code>IFile</code> provide a new writer entrance in which if the data is
 * less than 512 bytes, it piggybacks the data to the event; if the data size is
 * more than 512 bytes, it uses the fs stream based writer in <code>Writer</code>
 * inside <code>IFile</code>
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable


class FileBackedBoundedByteArrayOutputStream extends BoundedByteArrayOutputStream {
    boolean bufferIsFull;
    DataOutputStream out;
    Path file;

    FileBackedBoundedByteArrayOutputStream(Path file) {
        super(bufferLimit);
        this.file = file;
        this.fallback=fallback;
        this.limit=bufferLimit;
        this.bufferIsFull=false;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        IFileOutputStream checksumOut;
        CompressionOutputStream compressedOut;
        Compressor compressor;
        boolean compressOutput = false;

        if (bufferIsFull) {
            out.write(b, off, len);
        }
        try {
            super.write(b, off, len);
        } catch(EOFException e) {
            checksumOut = new IFileOutputStream(fs.create(file));
            compressor = CodecPool.getCompressor(codec);
            if (this.compressor != null) {
                this.compressor.reset();
                this.compressedOut = codec.createOutputStream(checksumOut, compressor);
                this.out = new FileBackedBoundedByteArrayOutputStream(file);
                this.compressOutput = true;
            } else {
                LOG.warn("Could not obtain compressor from CodecPool");
                out = new FSDataOutputStream(checksumOut,null);
                }
                bufferIsFull = true;
                out.write(getBuffer(), 0, len);
                out.write(b, off, len);
            }
    }

    public boolean hasSpilled() {
            return bufferIsFull;
        }

    }
