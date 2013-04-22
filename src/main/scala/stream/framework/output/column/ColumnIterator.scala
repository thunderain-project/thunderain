/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stream.framework.output.column

/**
 * Iterator interface for a column. The iterator should be initialized by a byte
 * buffer, and next can be invoked to get the value for each cell.
 */
abstract class ColumnIterator {

  def next()

  def current: Object
}


/**
 * A mapping from an integer column type to the respective ColumnIterator class.
 */
object ColumnIterator {

  // TODO: Implement Decimal data type.

  val COLUMN_TYPE_LENGTH = 8

  /////////////////////////////////////////////////////////////////////////////
  // List of data type sizes.
  /////////////////////////////////////////////////////////////////////////////
  val BOOLEAN_SIZE = 1
  val BYTE_SIZE = 1
  val SHORT_SIZE = 2
  val INT_SIZE = 4
  val LONG_SIZE = 8
  val FLOAT_SIZE = 4
  val DOUBLE_SIZE = 8
  val TIMESTAMP_SIZE = 12

  // Strings, binary types, and complex types (map, list) are assumed to be 16 bytes on average.
  val BINARY_SIZE = 16
  val STRING_SIZE = 8
  val COMPLEX_TYPE_SIZE = 16

  // Void columns are represented by NullWritable, which is a singleton.
  val NULL_SIZE = 0

  /////////////////////////////////////////////////////////////////////////////
  // List of column iterators.
  /////////////////////////////////////////////////////////////////////////////
  val BOOLEAN = 0
  val BYTE = 1
  val SHORT = 2
  val INT = 3
  val LONG = 4
  val FLOAT = 5
  val DOUBLE = 6
  val VOID = 7
  val TIMESTAMP = 8
  // TODO: Add decimal data type.

  val STRING = 10
  val COMPLEX = 11
  val BINARY = 12
}
