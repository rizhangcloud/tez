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
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class FileBackedBoundedByteArrayOutputStream extends FSDataOutputStream {
    //public class FileBackedBoundedByteArrayOutputStream extends FSDataOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FileBackedBoundedByteArrayOutputStream.class);
    static final byte[] HEADER = new byte[]{(byte) 'T', (byte) 'I',
            (byte) 'F', (byte) 0};
    boolean headerWritten = false;

    //BoundedByteArrayOutputStream memStream = new BoundedByteArrayOutputStream(512);

    //BoundedByteArrayOutputStream memStream;
    //FSDataOutputStream out;
    private BoundedByteArrayOutputStream out;

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


    public FileBackedBoundedByteArrayOutputStream(BoundedByteArrayOutputStream out, FileSystem.Statistics stats, FileSystem rfs,
                                                  Path file, CompressionCodec codec, boolean rle) {
        super(out, stats);

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

    /*
    public void write(int off) throws IOException {
        write(ByteBuffer.allocate(4).putInt(off).array(), (int) this.rawOut.getPos(), 4);
    }
     */

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (bufferIsFull) {
            outputStream.write(b, off, len);
        } else {
            if (out.available() > len) {
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

            outputStream.write(this.out.getBuffer());
            this.out.close();
            bufferIsFull = true;
        }
    }

    @Override
    public synchronized void write(int b) throws IOException {
            /* from the API, only 1 byte is written */
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
        return this.out.getBuffer();
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
            return this.out.getBuffer().length;
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

    public FileBackedBoundedByteArrayOutputStream(OutputStream out, FileSystem.Statistics stats, FileSystem rfs,
                                                  Path file, CompressionCodec codec, boolean rle) {
        super(out, stats);
        //this.memStream = new BoundedByteArrayOutputStream(512);

        this.fs = rfs;
        this.file = file;

        //this.limit=bufferLimit;
        this.bufferIsFull = false;

        this.codec = codec;
        this.rle = rle;
    }

    /* start implement */



    /**
     * Writes a string to the specified DataOutput using
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a>
     * encoding in a machine-independent manner.
     * <p>
     * First, two bytes are written to out as if by the <code>writeShort</code>
     * method giving the number of bytes to follow. This value is the number of
     * bytes actually written out, not the length of the string. Following the
     * length, each character of the string is output, in sequence, using the
     * modified UTF-8 encoding for the character. If no exception is thrown, the
     * counter <code>written</code> is incremented by the total number of
     * bytes written to the output stream. This will be at least two
     * plus the length of <code>str</code>, and at most two plus
     * thrice the length of <code>str</code>.
     *
     * @param      str   a string to be written.
     * @param      out   destination to write to
     * @return     The number of bytes written out.
     * @exception  IOException  if an I/O error occurs.
     */
    static int writeUTF(String str, DataOutput out) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535)
            throw new UTFDataFormatException(
                    "encoded string too long: " + utflen + " bytes");

        byte[] bytearr = null;
        if (out instanceof FileBackedBoundedByteArrayOutputStream) {
            FileBackedBoundedByteArrayOutputStream dos = (FileBackedBoundedByteArrayOutputStream)out;
            if(dos.bytearr == null || (dos.bytearr.length < (utflen+2)))
                dos.bytearr = new byte[(utflen*2) + 2];
            bytearr = dos.bytearr;
        } else {
            bytearr = new byte[utflen+2];
        }

        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

        int i=0;
        for (i=0; i<strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) break;
            bytearr[count++] = (byte) c;
        }

        for (;i < strlen; i++){
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        out.write(bytearr, 0, utflen+2);
        return utflen + 2;
    }



    /* end implement */



}
