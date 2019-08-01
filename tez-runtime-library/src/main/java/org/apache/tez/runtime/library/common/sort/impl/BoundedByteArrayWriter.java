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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
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
    private static final Logger LOG = LoggerFactory.getLogger(BoundedByteArrayWriter.class);
    protected FileBackedBoundedByteArrayOutputStream outputStream;


    /* The fifth constructor, which receives fs, and file, but does not create file. It uses the new
   stream.
 */
    public BoundedByteArrayWriter(Configuration conf, FileSystem rfs, Path file,
                  Class keyClass, Class valueClass,
                  CompressionCodec codec, TezCounter writesCounter, TezCounter serializedBytesCounter,
                  boolean rle, boolean dataViaEventEnabled) throws IOException {


        super(conf, rfs, file, keyClass, valueClass, codec, writesCounter,
                serializedBytesCounter, rle, dataViaEventEnabled);


        this.out = new FileBackedBoundedByteArrayOutputStream(null, null, rfs, file, codec, rle);
        this.hasOverflowed = ((FileBackedBoundedByteArrayOutputStream) this.out).hasOverflowed();

        //writeHeader(outputStream); // ??? moved inside the new stream

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

        /*??? handle the close in dataViaEvent case */


        if(this.hasOverflowed) {
            // write V_END_MARKER as needed
            writeValueMarker(out);

            // Write EOF_MARKER for key/value length
            WritableUtils.writeVInt(out, EOF_MARKER);
            WritableUtils.writeVInt(out, EOF_MARKER);
            decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
            //account for header bytes
            decompressedBytesWritten += HEADER.length;

            // Close the underlying stream iff we own it...
            if (ownOutputStream) {
                out.close();
            } else {
                boolean compressOutputTmp = ((FileBackedBoundedByteArrayOutputStream)out).getCompressOutput();

                if (compressOutputTmp) {
                    // Flush
                    CompressionOutputStream compressedOutTmp = ((FileBackedBoundedByteArrayOutputStream)out).getCompressedOut();
                    compressedOutTmp.finish();
                    compressedOutTmp.resetState();
                }
                // Write the checksum and flush the buffer
                ((FileBackedBoundedByteArrayOutputStream)out).getChecksumOut().finish();
                //checksumOut.finish();
            }
            //header bytes are already included in rawOut
            compressedBytesWritten = rawOut.getPos() - start;

            if (super.writtenRecordsCounter != null) {
                writtenRecordsCounter.increment(numRecordsWritten);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Total keys written=" + numRecordsWritten + "; rleEnabled=" + rle + "; Savings" +
                        "(due to multi-kv/rle)=" + totalKeySaving + "; number of RLEs written=" +
                        rleWritten + "; compressedLen=" + compressedBytesWritten + "; rawLen="
                        + decompressedBytesWritten);
            }
        }
        else {
            /* the case using in memory buffer */
            if (ownOutputStream) {
                out.close();
            } else {
                if (compressOutput) {
                    // Flush
                    compressedOut.finish();
                    compressedOut.resetState();
                }
                // Write the checksum and flush the buffer
                //checksumOut.finish();
            }
        }

        out = null;
        if (compressOutput) {
            // Return back the compressor
            CodecPool.returnCompressor(compressor);
            compressor = null;
        }

    }



    public byte[] getData() throws IOException {
        return outputStream.memStream.getBuffer();
    }

    /*??? */
    public long getRawLength() {
        if(this.outputStream.out instanceof FileBackedBoundedByteArrayOutputStream)
        {
            return outputStream.memStream.size();
        }
        else
        {
            return decompressedBytesWritten;
        }
    }

    /*??? assuming no compression in event data ? */
    public long getCompressedLength() {

        if(this.outputStream.out instanceof FileBackedBoundedByteArrayOutputStream)
        {
            return outputStream.memStream.size();
        }
        else
        {
            return compressedBytesWritten;
        }
    }


}

