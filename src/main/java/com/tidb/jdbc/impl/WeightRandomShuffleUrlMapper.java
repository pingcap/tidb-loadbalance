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
import java.util.function.Function;

public class WeightRandomShuffleUrlMapper implements Function<Bankend, String[]> {

  private Map<String, Weight> filter(Bankend bankend){
    Map<String, Weight> weightMap = new HashMap<>();
    Map<String,Weight> weightBankend = bankend.getWeightBankend();
    if(weightBankend == null){
      return weightMap;
    }
    if(weightBankend.size() == 0){
      return weightMap;
    }
    String[] backends = bankend.getBankend();
    Map<String, String> backendsMap = new HashMap<>();
    for (String url : backends){
      backendsMap.put(url,url);
    }
    weightBankend.forEach((k,v)->{
      if(backendsMap.containsKey(k)){
        weightMap.put(k,v);
      }
    });
    return weightMap;
  }


  /** @param bankend urls */
  @Override
  public String[] apply(final Bankend bankend) {
    Map<String, Weight> weightMap = filter(bankend);
    if(weightMap.size() == 0){
      return null;
    }
    int sumWeight = 0;
    Weight maxCurrentWeight = null;
    for (Weight weight : weightMap.values()){
      sumWeight += weight.getWeight();
      weight.setCurrentWeight(weight.getCurrentWeight() + weight.getWeight());
      if(maxCurrentWeight == null || maxCurrentWeight.getCurrentWeight() < weight.getCurrentWeight()){
        maxCurrentWeight = weight;
      }
    }
    List<Map.Entry<String,Weight>> list = new ArrayList<>(weightMap.entrySet());
    //升序排序
    list.sort((o1, o2) -> o2.getValue().getCurrentWeight().compareTo(o1.getValue().getCurrentWeight()));
    String[] result = new String[list.size()];
    for(int i=0;i<list.size();i++){
      result[i] = list.get(i).getKey();
    }
    maxCurrentWeight.setCurrentWeight(maxCurrentWeight.getCurrentWeight() - sumWeight);
    return result;
  }
}
