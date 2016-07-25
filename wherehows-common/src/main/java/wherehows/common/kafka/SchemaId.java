/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.common.kafka;

import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;


public class SchemaId {

  /**
   * Kafka Avro Schema ID type and size
   */
  public enum Type {
    INT(4), LONG(8), UUID(16);
    private int size;

    Type(int size) {
      this.size = size;
    }

    public int size() {
      return size;
    }
  }

  /**
   * get Schema ID from ByteBuffer by the type, move ByteBuffer position by size
   * @param byteBuffer
   * @return
   */
  public static String getIdString(Type type, ByteBuffer byteBuffer) {
    if (type == Type.INT) {
      return Integer.toString(byteBuffer.getInt());
    } else if (type == Type.LONG) {
      return Long.toString(byteBuffer.getLong());
    } else if (type == Type.UUID) {
      byte[] bytes = new byte[Type.UUID.size];
      byteBuffer.get(bytes);
      return DatatypeConverter.printHexBinary(bytes).toLowerCase();
    } else {
      return null;
    }
  }

  /**
   * convert String id into ByteBuffer
   * @param type
   * @param id String
   * @return byte[]
   */
  public static byte[] getIdBytes(Type type, String id) {
    if (type == Type.INT) {
      return ByteBuffer.allocate(Type.INT.size).putInt(Integer.parseInt(id)).array();
    } else if (type == Type.LONG) {
      return ByteBuffer.allocate(Type.LONG.size).putLong(Long.parseLong(id)).array();
    } else if (type == Type.UUID) {
      return DatatypeConverter.parseHexBinary(id);
    } else {
      return new byte[0];
    }
  }
}
