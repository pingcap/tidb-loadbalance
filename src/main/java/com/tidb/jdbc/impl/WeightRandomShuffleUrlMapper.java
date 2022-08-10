/*
 * Copyright 2020 TiDB Project Authors.
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

package com.tidb.jdbc.impl;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class WeightRandomShuffleUrlMapper implements Function<Bankend, String[]> {

  private final AtomicLong offset = new AtomicLong();

  /** @param input urls */
  @Override
  public String[] apply(final Bankend bankend) {
    Random random = ThreadLocalRandom.current();
    List<String> bankendArray = new ArrayList<>();
    Map<String,Integer> weightMap = bankend.getWeightBankend();
    if(weightMap == null){
      return null;
    }
    if(weightMap.size() == 0){
      return null;
    }
    weightMap.forEach((k,v)->{
      if(v != 0){
        for (int i = 0;i< v;i++){
          bankendArray.add(k);
        }
      }
    });
    String[] input = bankendArray.toArray(new String[bankendArray.size()]);
    int currentOffset = (int)(this.offset.getAndIncrement() % (long)input.length);
    if (currentOffset == 0) {
      return input;
    } else {
      String[] result = new String[input.length];
      int right = input.length - currentOffset;
      System.arraycopy(input, currentOffset, result, 0, right);
      System.arraycopy(input, 0, result, right, currentOffset);
      return result;
    }
  }
}
