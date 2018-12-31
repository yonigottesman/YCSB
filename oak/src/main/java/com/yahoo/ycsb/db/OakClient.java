/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.oath.oak.OakIterator;
import com.oath.oak.OakMap;
import com.oath.oak.OakMapBuilder;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


/**
 * Oak client.
 */
public class OakClient extends DB{

  private OakMap<String, String> oakMap;

  @Override
  public void init() throws DBException {


    Properties props = getProperties();

    OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
        .setKeySerializer(new StringSerializer())
        .setValueSerializer(new StringSerializer())
        .setComparator(new StringComparator())
        .setChunkBytesPerItem(64)
        .setMinKey("");


    oakMap = builder.build();
  }
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String value = oakMap.get(key);
    result.put(key, new StringByteIterator(value));
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,
      ByteIterator>> result) {
    OakIterator<Map.Entry<String, String>> iterator = oakMap.entriesIterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> kv = iterator.next();
      HashMap<String, ByteIterator> map = new HashMap<>();
      map.put(kv.getKey(), new StringByteIterator(kv.getValue()));
      result.add(map);
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    oakMap.put(key, values.values().iterator().next().toString());
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    oakMap.remove(key);
    return Status.OK;
  }
}
