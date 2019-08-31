/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.*;
import java.util.function.Supplier;

import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

/**
 * <code>FileBackedBoundedByteArrayOutputStream</code> extends the <code> BoundedByteArrayOutputStream</code>
 * provide a new write  in which if the data is
 * less than 512 bytes, it piggybacks the data to the event; if the data size is
 * more than 512 bytes, it uses the fs stream based writer in <code>Writer</code>
 * inside <code>IFile</code>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class FileBackedBoundedByteArrayOutputStream extends OutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FileBackedBoundedByteArrayOutputStream.class);

    ByteArrayOutputStream out;
    FileSystem fs;
    Supplier<Path> pathSupplier;
    int headerLength;
    boolean bufferIsFull;
    FSDataOutputStream baseStream;
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
    private int bufferSize = 0;
    private int written;
    private byte[] singleByte = new byte[1];
    private final DataChecksum sum;
    private byte[] barray;
    private byte[] buffer;
    private int offset;
    private boolean closed = false;
    private boolean finished = false;


    public FileBackedBoundedByteArrayOutputStream(ByteArrayOutputStream out, FileSystem.Statistics stats,
                                                  FileSystem rfs, Supplier<Path> sFile, CompressionCodec codec,
                                                  boolean rle, int bufferLimit, int headerLength) {
        this.bufferSize = bufferLimit;
        this.out = out;
        this.fs = rfs;
        this.pathSupplier = sFile;
        this.headerLength = headerLength;
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

        // checksum initialization
        sum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32,
                Integer.MAX_VALUE);
        barray = new byte[sum.getChecksumSize()];
        buffer = new byte[4096];
        offset = 0;


        //Todo debug use, should delete
        LOG.info("dataviavent: FileBackedBoundedByteArrayOutputStream constructor");
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (bufferIsFull) {
            outputStream.write(b, off, len);
        } else {
            if ((this.bufferSize -out.size()) > len) {
                out.write(b, off, len);
                checksum(b, off, len);
                written += len;
                return;
            } else {

                //Todo debug use, should delete
                LOG.info("dataviavent: file is created by fs.create");

                baseStream = fs.create(pathSupplier.get());
                this.rawOut = baseStream;
                this.checksumOut = new IFileOutputStream(baseStream);
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

            /* If the in memory buffer is full, switched to the file based approach，and
            clone the data from the in memory buffer into the file based stream.
            Not necessary to compute the checksum for the data in the in memory buffer, because
            the in memory buffer will be cloned into the checksum based stream. */
            finished = true;
            this.out.flush();

            /* clone the data from the in memory buffer into the file based stream */
            byte[] inMemData = this.out.toByteArray();
            baseStream.write(inMemData, 0, this.headerLength);
            outputStream.write(inMemData, this.headerLength, inMemData.length-this.headerLength);

            // write the data for which the in memory does not have enough space to the file based stream
            outputStream.write(b, off, len);
            bufferIsFull = true;

            this.out.close();

            //Todo debug use, should delete
            LOG.info("dataviavent: in memory buffer is full");

        }
    }

    /**
     * Writes the specified byte to this output stream.
     * Only the lower eight bits will be written into the output stream, and the higher 24 bits are
     * ignored. This method is required by all the subclasses of <code>OutputStream</code>
     * one byte will be written into the output stream.
     */
    @Override
    public synchronized void write(int b) throws IOException {
            /* from the API, only 1 byte is written */
            singleByte[0] = (byte) b;
            write(singleByte, 0, singleByte.length);
    }

    @Override
    public void close() throws IOException {
        if (!bufferIsFull) {
            closed = true;
            finish();
            this.out.close();
        }
        else {

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

    public long getStart() {
        return start;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    /**
     * Finishes writing data to the output stream, by writing
     * the checksum bytes to the end. The underlying stream is not closed.
     * @throws IOException
     */
    public void finish() throws IOException {
        if (finished) {
            return;
        }
        finished = true;
        sum.update(buffer, 0, offset);
        sum.writeValue(barray, 0, false);
        out.write (barray, 0, sum.getChecksumSize());
        out.flush();
    }
    private void checksum(byte[] b, int off, int len) {
        if(len >= buffer.length) {
            sum.update(buffer, 0, offset);
            offset = 0;
            sum.update(b, off, len);
            return;
        }
        final int remaining = buffer.length - offset;
        if(len > remaining) {
            sum.update(buffer, 0, offset);
            offset = 0;
        }
    /*
    // FIXME if needed re-enable this in debug mode
    if (LOG.isDebugEnabled()) {
      LOG.debug("XXX checksum" +
          " b=" + b + " off=" + off +
          " buffer=" + " offset=" + offset +
          " len=" + len);
    }
    */
        /* now we should have len < buffer.length */
        System.arraycopy(b, off, buffer, offset, len);
        offset += len;
    }
}
