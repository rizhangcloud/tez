package org.apache.tez.runtime.library.common.sort.impl;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.tez.common.counters.TezCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.hadoop.conf.Configuration;


@InterfaceAudience.Private
@InterfaceStability.Unstable

public class BoundedByteArrayWriter extends Writer {
    /*
    //public class FileBackedBoundedByteArrayOutputStream extends FSDataOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FileBackedBoundedByteArrayOutputStream.class);

    private static final Logger LOG = LoggerFactory.getLogger(BoundedByteArrayWriter.class);
    protected FileBackedBoundedByteArrayOutputStream outputStream;


    static final byte[] HEADER = new byte[]{(byte) 'T', (byte) 'I',
            (byte) 'F', (byte) 0};
    boolean headerWritten = false;

    //BoundedByteArrayOutputStream memStream = new BoundedByteArrayOutputStream(512);

    //BoundedByteArrayOutputStream memStream;
    //FSDataOutputStream out;

    ByteArrayOutputStream out;

    private FSDataOutputStream internalBuffer;

    FileSystem fs;
    Path file;

    boolean bufferIsFull;

    FSDataOutputStream outputStream;

    FSDataOutputStream rawOut;

    CompressionCodec codec;

    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;

    // de-dup keys or not
    protected final boolean rle;

    IFileOutputStream checksumOut;
    long start = 0;

    boolean ownOutputStream = false;

    private int bufferSize = 0;

    private int currentPointer;
    private int written;
    private int startOffset;

    private byte[] singleByte = new byte[1];

    private byte[] bytearr = null;


    public FileBackedBoundedByteArrayOutputStream(ByteArrayOutputStream out, FileSystem.Statistics stats, FileSystem rfs,
                                                  Path file, CompressionCodec codec, boolean rle, int bufferLimit) {
        super(out, stats);
        this.bufferSize = bufferLimit;

        this.out = out;

        this.fs = rfs;
        this.file = file;

        //this.limit=bufferLimit;
        this.bufferIsFull = false;

        this.codec = codec;
        this.rle = rle;
        if (this.codec != null) {
            this.compressor = CodecPool.getCompressor(codec);
            if (this.compressor != null) {
                compressOutput = true;
            }
        }
        written = 0;
    }


    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (bufferIsFull) {
            outputStream.write(b, off, len);
        } else {
            if ((this.bufferSize -out.size()) > len) {
                out.write(b, off, len);
                written += len;
                return;
            } else {
                outputStream = fs.create(file);
                this.rawOut = outputStream;
                this.checksumOut = new IFileOutputStream(outputStream);
                this.start = this.rawOut.getPos();

                if (compressOutput) {
                    this.compressedOut = codec.createOutputStream(this.checksumOut, this.compressor);
                    this.outputStream = new FSDataOutputStream(this.compressedOut, null);
                    this.compressOutput = true;
                    written += this.rawOut.getPos() - written;
                } else {
                    LOG.warn("Could not obtain compressor from CodecPool");
                    this.outputStream = new FSDataOutputStream(checksumOut, null);
                    written += len;
                }
            }

            //writeHeader(this.outputStream);  //??? necessary

            outputStream.write(this.out.toByteArray());
            this.out.close();
            bufferIsFull = true;
        }
    }

    @Override
    public synchronized void write(int b) throws IOException {
        //from the API, only 1 byte is written
        singleByte[0] = (byte)b;
        write(singleByte, 0, singleByte.length);

        //write(ByteBuffer.allocate(1).putInt(b).array(), written, ByteBuffer.allocate(4).putInt(b).capacity());

            /*
            if(totalBytes < out.getBuffer().length)
            {
                write(ByteBuffer.allocate(4).putInt(b).array(), totalBytes, ByteBuffer.allocate(4).putInt(b).capacity());
                //totalBytes += ByteBuffer.allocate(4).putInt(b).capacity();
            }
            else
            {
                outputStream.write(b);
                totalBytes += ByteBuffer.allocate(4).putInt(b).capacity();
                //not compress yet

            }
            */
    /*
    }

    @Override
    public void close() throws IOException {
        if (!bufferIsFull)
            this.out.close();
        else {
            this.outputStream.close();
            if (compressOutput) {
                // Flush
                compressedOut.finish();
                compressedOut.resetState();
            }
            // Write the checksum and flush the buffer
            checksumOut.finish();
        }
    }

    public byte[] getBuffer() throws IOException {
        return this.out.toByteArray();
    }


    public boolean hasOverflowed() {
        return bufferIsFull;
    }

    public long getCompressedBytesWritten() {
        if(bufferIsFull){
            return this.rawOut.getPos() - this.start;
        }
        else
        {
            return this.out.size();
        }
    }

    public FSDataOutputStream getRawOut() {
        return rawOut;
    }

    public long getStart() {
        return start;
    }


    public CompressionCodec getCodec() {
        return codec;
    }

    public CompressionOutputStream getCompressedOut() {
        return compressedOut;
    }

    public Compressor getCompressor() {
        return compressor;
    }

    public boolean getCompressOutput() {
        return compressOutput;
    }

    // de-dup keys or not
    public boolean getRle() {
        return rle;
    }

    public IFileOutputStream getChecksumOut() {
        return checksumOut;
    }

     */


}
