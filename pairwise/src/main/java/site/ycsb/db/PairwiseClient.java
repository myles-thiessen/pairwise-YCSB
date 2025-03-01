/**
 * Copyright (c) 2013 - 2016 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import mthiessen.experiments.clients.RemoteClient;
import mthiessen.statemachines.RocksDBStateMachine;
import site.ycsb.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/** */
public class PairwiseClient extends DB {

  public static final String IP_PROPERTY = "pairwise.ip";

  public static final String PORT_PROPERTY = "pairwise.port";

  private RemoteClient client;

  @Override
  public void init() throws DBException {
    Properties prop = getProperties();

    String ip = prop.getProperty(IP_PROPERTY);

    String port = prop.getProperty(PORT_PROPERTY);

    this.client = new RemoteClient(ip, Integer.parseInt(port));
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    RocksDBStateMachine.ReadRequest readRequest = new RocksDBStateMachine.ReadRequest(table, key);

    byte[] response = this.client.read(readRequest);
    if (response == null) {
      return Status.NOT_FOUND;
    }
    deserializeValues(response, fields, result);
    return Status.OK;
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    return Status.SERVICE_UNAVAILABLE;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      RocksDBStateMachine.WriteRequest writeRequest =
          new RocksDBStateMachine.WriteRequest(table, key, serializeValues(values));

      this.client.insert(writeRequest);

      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      RocksDBStateMachine.WriteRequest writeRequest =
          new RocksDBStateMachine.WriteRequest(table, key, serializeValues(values));

      this.client.insert(writeRequest);

      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.SERVICE_UNAVAILABLE;
  }

  private Map<String, ByteIterator> deserializeValues(
      final byte[] values, final Set<String> fields, final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
}
