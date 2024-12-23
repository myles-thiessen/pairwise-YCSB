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

import mthiessen.experiments.ExperimentClient;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;

/**
 *
 */
public class PairwiseClient extends DB {

  public static final String IP_PROPERTY = "pairwise.ip";

  public static final String PORT_PROPERTY = "pairwise.port";

  private ExperimentClient client;

  @Override
  public void init() throws DBException {
    Properties prop = getProperties();

    String ip = prop.getProperty(IP_PROPERTY);

    String port = prop.getProperty(PORT_PROPERTY);

    this.client = new ExperimentClient(ip, Integer.parseInt(port));
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    this.client.read();

    return Status.OK;
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    this.client.read();

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    this.client.write();

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    this.client.write();

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    this.client.write();

    return Status.OK;
  }
}
