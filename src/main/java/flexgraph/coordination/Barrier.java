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
 * An interface for barrier between distributed workers.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public interface Barrier {
    /**
     * Initializes this barrier.
     */
    void setup();

    /**
     * Marks the barrier for this worker as completed, and notifies this changes
     * to all other workers.
     */
    void markCompleted();

    /**
     * Blocks until all other workers are completed.
     */
    void waitBarrier();

    /**
     * Finalizes this barrier.
     *
     * After calling this method, any calling method in this class is ignored.
     */
    void cleanup();
}
