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
package flexgraph.types;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * A class that represents a matrix column in double-valued matrix.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class WeightedColumnEntry {
    public int col;
    public int numEntries;
    public IntArrayList rows;
    public DoubleArrayList weights;
}
