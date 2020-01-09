/*
 * Copyright 2018 SNU Data Mining Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package flexgraph.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import flexgraph.io.VectorRecordReader;
import flexgraph.io.VectorRecordWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A utility class that provides efficient I/O methods.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class IOUtils {
    private static CompressionCodec preferredCodec = null;

    public static Path getIntermediateVectorPath(
            final TaskAttemptContext context, final String prefix, final int blockId) throws IOException {
        final Path rootPath = new Path(FileOutputFormat.getOutputPath(context), "intermediate");
        return new Path(rootPath, String.format("%s-%05d", prefix, blockId));
    }

    public static <T extends Writable> RecordWriter<IntWritable, T> getIntermediateVectorWriter(
            final TaskAttemptContext context, final String prefix, final int blockId)
            throws IOException {
        return getIntermediateVectorWriter(context, getIntermediateVectorPath(context, prefix, blockId));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Writable> RecordWriter<IntWritable, T> getIntermediateVectorWriter(
            final TaskAttemptContext context, final Path path) throws IOException {
        final Configuration conf = context.getConfiguration();
        final int bufSize = conf.getInt("io.file.buffer.size", 4096);
        final FileSystem fs = path.getFileSystem(conf);

        final OutputStream out = fs.create(path, true, bufSize, (short) 1, fs.getDefaultBlockSize(path), context);
        return new VectorRecordWriter<>(out, context);
    }

    public static <T extends Writable> RecordReader<IntWritable, T> getIntermediateVectorReader(
            final TaskAttemptContext context, final String prefix, final int blockId, final Class<T> valueClass)
            throws IOException {
        return getIntermediateVectorReader(context, getIntermediateVectorPath(context, prefix, blockId), valueClass);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Writable> RecordReader<IntWritable, T> getIntermediateVectorReader(
            final TaskAttemptContext context, final Path path, final Class<T> valueClass)
            throws IOException {
        final Configuration conf = context.getConfiguration();
        final FileSystem fs = path.getFileSystem(conf);

        if (!fs.exists(path) || fs.getFileStatus(path).getLen() == 0) return null;
        final FSDataInputStream in = fs.open(path);

        return new VectorRecordReader<>(in, context, valueClass);
    }

    public static Path getLocalTemporaryPath(final String name, final TaskAttemptContext context) {
        return getLocalTemporaryPath(name, context.getJobID().getId());
    }

    public static Path getLocalTemporaryPath(final String name, final int id) {
        return getLocalTemporaryPath(String.format("%s-%d", name, id));
    }

    public static Path getLocalTemporaryPath(final String name) {
        return new Path(System.getProperty("java.io.tmpdir"), name);
    }

    public static DataInputStream getTemporaryFileInputStream(final Path path, final TaskAttemptContext context)
            throws IOException {
        final FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        final CompressionCodec codec = getPreferredCompressionCodec(context);

        return new DataInputStream(codec.createInputStream(fs.open(path)));
    }

    public static DataOutputStream getTemporaryFileOutputStream(final Path path, final TaskAttemptContext context)
            throws IOException {
        final FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        final CompressionCodec codec = getPreferredCompressionCodec(context);

        return new DataOutputStream(codec.createOutputStream(fs.create(path, (short) 1)));
    }

    public static CompressionCodec getPreferredCompressionCodec(final Configuration conf) {
        if (preferredCodec != null) {
            return preferredCodec;
        }
        Class<? extends CompressionCodec> codecClass;
        if (Lz4Codec.isNativeCodeLoaded()) {
            codecClass = Lz4Codec.class;
        } else if (SnappyCodec.isNativeCodeLoaded()) {
            codecClass = SnappyCodec.class;
        } else {
            codecClass = DefaultCodec.class;
        }

        preferredCodec = ReflectionUtils.newInstance(codecClass, conf);
        return preferredCodec;
    }

    public static CompressionCodec getPreferredCompressionCodec(final TaskAttemptContext context) {
        return getPreferredCompressionCodec(context.getConfiguration());
    }
}
