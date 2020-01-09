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

/**
 * A utility class that manages Java heap.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class MemoryUtils {
    private static final Runtime JavaRuntime = Runtime.getRuntime();

    public static void callGCMultipleTimes() {
        callGCMultipleTimes(5);
    }

    public static void callGCMultipleTimes(int times) {
        for (int i = 0; i < times; ++i) {
            JavaRuntime.gc();
        }
    }

    public static long maxMemoryBytes() {
        return JavaRuntime.maxMemory();
    }

    public static long availableMemoryBytes() {
        return JavaRuntime.maxMemory() - JavaRuntime.totalMemory() + JavaRuntime.freeMemory();
    }

    public static long bytesToMebibytes(final long bytes) {
        return (long) (bytes / 1024.0 / 1024.0);
    }
}
