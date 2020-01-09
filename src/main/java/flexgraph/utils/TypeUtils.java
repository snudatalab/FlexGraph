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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A utility class that provides byte-level type utilities.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class TypeUtils {

    public static class LongComparator extends WritableComparator {
        public LongComparator(Class<? extends WritableComparable> keyClass) {
            super(keyClass);
        }

        @Override
        public int compare(final byte[] b1, final int s1, final int l1, final byte[] b2, final int s2, final int l2) {
            final long id1 = readLong(b1, s1);
            final long id2 = readLong(b2, s2);
            return id1 < id2 ? -1 : id1 > id2 ? 1 : 0;
        }
    }

    public static class IntComparator extends WritableComparator {
        public IntComparator(Class<? extends WritableComparable> keyClass) {
            super(keyClass);
        }

        @Override
        public int compare(final byte[] b1, final int s1, final int l1, final byte[] b2, final int s2, final int l2) {
            final int intValue1 = readInt(b1, s1);
            final int intValue2 = readInt(b2, s2);
            return intValue1 < intValue2 ? -1 : intValue1 > intValue2 ? 1 : 0;
        }
    }
}
