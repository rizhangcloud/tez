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
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;
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
 * <code>FileBackedBoundedByteArrayOutputStream</code> extends the <code> BoundedByteArrayOutputStream</code>
 * provide a new write  in which if the data is
 * less than 512 bytes, it piggybacks the data to the event; if the data size is
 * more than 512 bytes, it uses the fs stream based writer in <code>Writer</code>
 * inside <code>IFile</code>
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class FileBackedBoundedByteArrayOutputStream extends FSDataOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FileBackedBoundedByteArrayOutputStream.class);
    static final byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I',
            (byte) 'F' , (byte) 0};
    boolean headerWritten = false;

    BoundedByteArrayOutputStream memStream;
    FSDataOutputStream out;
    FileSystem fs;
    Path file;

    boolean bufferIsFull;

    FSDataOutputStream rawOut;


    CompressionCodec codec;

    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;

    // de-dup keys or not
    protected final boolean rle;

    IFileOutputStream checksumOut;
    long start = 0;



    public FileBackedBoundedByteArrayOutputStream(OutputStream out, FileSystem.Statistics stats,FileSystem rfs,
                                                  Path file, CompressionCodec codec, boolean rle) {
        super(out, stats);
        this.fs = rfs;
        this.file = file;

        this.memStream = new BoundedByteArrayOutputStream(512);
        //this.limit=bufferLimit;
        this.bufferIsFull=false;

        this.codec = codec;
        this.rle = rle;
    }



    @Override
    public void write(byte[] b, int off, int len) throws IOException {

        FSDataOutputStream outputStream;

        if (bufferIsFull) {
            out.write(b, off, len);
        }
        try {
            memStream.write(b, off, len);
        } catch(EOFException e) {
            /* The data in the buffer is over the limit, so creates a file based stream */
            outputStream = fs.create(file);
            this.rawOut = outputStream;
            this.checksumOut = new IFileOutputStream(outputStream);

            this.start = this.rawOut.getPos(); //??? how to return to the IFile.writer caller?

            if (this.codec != null) {
                this.compressor = CodecPool.getCompressor(codec);
                if (this.compressor != null) {
                    this.compressor.reset();
                    this.compressedOut = codec.createOutputStream(this.checksumOut, this.compressor);
                    this.out = new FSDataOutputStream(this.compressedOut,  null);
                    this.compressOutput = true;
                } else {
                    LOG.warn("Could not obtain compressor from CodecPool");
                    this.out = new FSDataOutputStream(checksumOut,null);
                }
            } else {
                this.out = new FSDataOutputStream(checksumOut,null);
            }
            writeHeader(this.out);
            /* end of creating file based stream */

            bufferIsFull = true;
            out.write(b, off, len);
            }
    }

    protected void writeHeader(OutputStream outputStream) throws IOException {
        if (!headerWritten) {
            outputStream.write(HEADER, 0, HEADER.length - 1);
            outputStream.write((compressOutput) ? (byte) 1 : (byte) 0);
            headerWritten = true;
        }
    }

    public boolean hasSpilled() {
            return bufferIsFull;
        }

    public void close() throws IOException {
        /* if the buffer is full, close the memory buffer */
        if(bufferIsFull)
            this.out.close();
    }

}
