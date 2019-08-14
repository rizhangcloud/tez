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
import org.apache.hadoop.io.serializer.SerializationFactory;
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
    //public class FileBackedBoundedByteArrayOutputStream extends FSDataOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedByteArrayWriter.class);
    //protected FileBackedBoundedByteArrayOutputStream outputStream;

    static final byte[] HEADER = new byte[]{(byte) 'T', (byte) 'I',
            (byte) 'F', (byte) 0};

    //BoundedByteArrayOutputStream memStream = new BoundedByteArrayOutputStream(512);
    //BoundedByteArrayOutputStream memStream;
    //FSDataOutputStream out;

    ByteArrayOutputStream out;


    FileSystem fs;
    Path file;

    CompressionCodec codec;

    boolean bufferIsFull;

    FSDataOutputStream outputStream;

    private int bufferSize = 0;

    private int written;

    private byte[] singleByte = new byte[1];


    public BoundedByteArrayWriter(ByteArrayOutputStream out, FileSystem.Statistics stats, FileSystem rfs,
                                  Path file, CompressionCodec codec, boolean rle, int bufferLimit) {
        super(out, stats);
        this.bufferSize = bufferLimit;

        this.out = out;

        this.fs = rfs;
        this.file = file;

        this.bufferIsFull = false;

        this.codec = codec;

        if (this.codec != null) {
            this.compressor = CodecPool.getCompressor(codec);
            if (this.compressor != null) {
                compressOutput = true;
            }
        }
        written = 0;
    }


    /* The fifth constructor, which receives fs, and file, but does not create file. It uses the new
   stream.
 */
    //public Writer(Configuration conf, FileSystem rfs, Path file,
    public BoundedByteArrayWriter(Configuration conf, FileSystem rfs, Path file,
                  Class keyClass, Class valueClass,
                  CompressionCodec codec, TezCounter writesCounter, TezCounter serializedBytesCounter,
                  boolean rle, boolean dataViaEventEnabled) throws IOException {

        //this.rawOut = outputStream;//???: how to get the rawout?
        this.writtenRecordsCounter = writesCounter;
        this.serializedUncompressedBytes = serializedBytesCounter;

        //this.start = this.rawOut.getPos(); //???
        this.start = 0;
        this.rle = rle;

        //this.out=new FileBackedBoundedByteArrayOutputStream(this.compressedOut, null, file, codec, rle);
        ByteArrayOutputStream memStream = new ByteArrayOutputStream(512);
        //this.out = new FileBackedBoundedByteArrayOutputStream(null, null, rfs, file, codec, rle);

        this.out = new FileBackedBoundedByteArrayOutputStream(memStream, null, rfs, file, codec, rle, 4096);

        //this.hasOverflowed = ((FileBackedBoundedByteArrayOutputStream) this.out).hasOverflowed();

        /* ??? write the header from the beginning */
        writeHeader(memStream);

        if (keyClass != null) {
            this.closeSerializers = true;
            SerializationFactory serializationFactory =
                    new SerializationFactory(conf);
            this.keySerializer = serializationFactory.getSerializer(keyClass);
            this.keySerializer.open(buffer);
            this.valueSerializer = serializationFactory.getSerializer(valueClass);
            this.valueSerializer.open(buffer);
        } else {
            this.closeSerializers = false;
        }
    }

    public boolean InMemBuffer() {

            if (hasOverflowed())
                return false;
            else
                return true;
    }


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

                    /* ??? no compression in in memory case. CompressOutput true always? */
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

                outputStream.write(this.out.toByteArray());
                this.out.close();
                bufferIsFull = true;
            }
        }



    public synchronized void write(int b) throws IOException {
        /* from the API, only 1 byte is written */
        singleByte[0] = (byte)b;
        write(singleByte, 0, singleByte.length);
    }


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

    public void close() throws IOException {
        if (closed.getAndSet(true)) {
            throw new IOException("Writer was already closed earlier");
        }

        // When IFile writer is created by BackupStore, we do not have
        // Key and Value classes set. So, check before closing the
        // serializers
        if (closeSerializers) {
            keySerializer.close();
            valueSerializer.close();
        }

        // write V_END_MARKER as needed
        writeValueMarker(out);

        // Write EOF_MARKER for key/value length
        WritableUtils.writeVInt(out, EOF_MARKER);
        /* entrance: write the EOF */
        /* ??? write checksum for in memory buffer */
      /*
        if(out instanceof FileBackedBoundedByteArrayOutputStream)
        {
            CRC32 crc = new CRC32();
            crc.update(((FileBackedBoundedByteArrayOutputStream) out).getBuffer());
            WritableUtils.writeVLong(out, crc.getValue());
        }
       */

        WritableUtils.writeVInt(out, EOF_MARKER);
        decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
        //account for header bytes
        decompressedBytesWritten += HEADER.length;


        // Close the underlying stream iff we own it...
        if (ownOutputStream) {
            out.close();
        } else {
            if (!(out instanceof FileBackedBoundedByteArrayOutputStream)) {
                if (compressOutput) {
                    // Flush
                    compressedOut.finish();
                    compressedOut.resetState();
                }
                // Write the checksum and flush the buffer
                checksumOut.finish();
            }
        }

        //header bytes are already included in rawOut
        if( out instanceof FileBackedBoundedByteArrayOutputStream)
        {
            ((FileBackedBoundedByteArrayOutputStream) out).getCompressedBytesWritten();
        }
        else
            compressedBytesWritten = rawOut.getPos() - start;

        if (compressOutput) {
            // Return back the compressor
            CodecPool.returnCompressor(compressor);
            compressor = null;
        }

        /* store the data to be placed in event payload */
        if( out instanceof FileBackedBoundedByteArrayOutputStream)
        {
            this.tmpDataBuffer  =
                    new byte[((FileBackedBoundedByteArrayOutputStream) this.getOutputStream()).getBuffer().length];
            System.arraycopy(((FileBackedBoundedByteArrayOutputStream) this.getOutputStream()).getBuffer(),
                    0, tmpDataBuffer, 0,
                    ((FileBackedBoundedByteArrayOutputStream) this.getOutputStream()).getBuffer().length);

        }


        out = null;

        if (writtenRecordsCounter != null) {
            writtenRecordsCounter.increment(numRecordsWritten);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Total keys written=" + numRecordsWritten + "; rleEnabled=" + rle + "; Savings" +
                    "(due to multi-kv/rle)=" + totalKeySaving + "; number of RLEs written=" +
                    rleWritten + "; compressedLen=" + compressedBytesWritten + "; rawLen="
                    + decompressedBytesWritten);
        }

    }
    public long getRawLength() {
        if (out instanceof FileBackedBoundedByteArrayOutputStream) {
            //return ((FileBackedBoundedByteArrayOutputStream) out).memStream.size();
            return ((FileBackedBoundedByteArrayOutputStream) out).size();
        } else {
            return decompressedBytesWritten;
        }
    }

    /*??? assuming no compression in dataViaEvent ? */
    public long getCompressedLength() {

        if(out instanceof FileBackedBoundedByteArrayOutputStream)
        {
            //return ((FileBackedBoundedByteArrayOutputStream) out).memStream.size();
            return ((FileBackedBoundedByteArrayOutputStream) out).size();
        }
        else
        {
            return compressedBytesWritten;
        }
    }

    public byte[] getTmpDataBuffer() {
        return tmpDataBuffer;
    }

    /* ??? originally implemented in FileBasedKVWriter. To enable dataViaEvent, add it back */
    /*
    public byte[] getData() throws IOException {

      Preconditions.checkState(this.closed,
              "Only available after the Writer has been closed");


      if(this.getOutputStream() instanceof FileBackedBoundedByteArrayOutputStream)
      {
        return ((FileBackedBoundedByteArrayOutputStream)(this.getOutputStream())).getBuffer();
      }
      //else
        //return null;
      else {
        //m the old stream
        FSDataInputStream inStream = null;

        byte[] buf = null;
        try {

          inStream = rfs.open(finalOutPath); //??? finalOutPath
          buf = new byte[(int) getCompressedLength()];
          IOUtils.readFully(inStream, buf, 0, (int) getCompressedLength());
        } finally {
          if (inStream != null) {
            inStream.close();
          }
        }
        return buf;
      }
    }*/


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

}
