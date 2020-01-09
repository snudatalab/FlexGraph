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
package flexgraph.cache;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import flexgraph.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * A base implementation for MatrixIterator.
 *
 * @see flexgraph.cache.MatrixIterator
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public abstract class BaseMatrixIterator<V extends Writable> implements MatrixIterator<V> {
    protected final Input in;
    protected final KryoDataInput dataIn;
    private final Decompressor decompressor;

    public BaseMatrixIterator(final Path path, final Configuration conf) throws IOException {
        if (path != null) {
            final LocalFileSystem localFS = FileSystem.getLocal(conf);
            final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(conf);
            final InputStream rawStream = localFS.open(path);
            decompressor = CodecPool.getDecompressor(codec);
            in = new Input(codec.createInputStream(rawStream, decompressor));
            dataIn = new KryoDataInput(in);
        } else {
            in = null;
            dataIn = null;
            decompressor = null;
        }
    }

    @Override
    public void close() throws Exception {
        if (in != null) {
            in.close();
            CodecPool.returnDecompressor(decompressor);
        }
    }
}
