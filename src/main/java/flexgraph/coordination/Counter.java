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
package flexgraph.coordination;

/**
 * An interface for counter between distributed workers.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public interface Counter<T> {
    /**
     * Updates this counter with given value.
     *
     * @param value the value to be updated
     */
    void update(T value);

    /**
     * Returns the minimum value of all distributed counters.
     *
     * @return the minimum value
     */
    T getMinimum();

    /**
     * Returns the maximum value of all distributed counters.
     *
     * @return the maximum value
     */
    T getMaximum();

    /**
     * Returns the sum of values in all distributed counters.
     *
     * @return the sum of values
     */
    T getSum();
}
