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

import java.nio.ByteBuffer

import javaewah.EWAHCompressedBitmap
import javaewah.EWAHCompressedBitmapSerializer


trait ColumnBuilder[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T] {

  def append(v: T)

  def appendNull()

  def build: ByteBuffer

  // Subclasses should call super.initialize to initialize the null bitmap.
  def initialize(initialSize: Int) {
    _nullBitmap = new EWAHCompressedBitmap
  }

  protected var _nullBitmap: EWAHCompressedBitmap = null

  protected def sizeOfNullBitmap: Int = 8 + EWAHCompressedBitmapSerializer.sizeof(_nullBitmap)

  protected def writeNullBitmap(buf: ByteBuffer) = {
    if (_nullBitmap.cardinality() > 0) {
      buf.putLong(1L)
      EWAHCompressedBitmapSerializer.writeToBuffer(buf, _nullBitmap)
    } else {
      buf.putLong(0L)
    }
  }
}
