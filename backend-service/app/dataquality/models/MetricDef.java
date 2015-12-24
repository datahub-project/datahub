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
package dataquality.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.type.TypeFactory;
import dataquality.dao.AggCombDao;
import dataquality.dq.AuxiliaryOp;
import dataquality.dq.DqRule;
import java.util.List;


/**
 * Created by zechen on 8/3/15.
 */
public class MetricDef {

  int id;
  String name;
  int aggDefId;
  AggComb comb;
  AggComb auxComb;
  AuxiliaryOp auxOp;
  List<DqRule> rules;

  @JsonIgnore
  JsonNode originalJson;

  public MetricDef(JsonNode json) throws IllegalArgumentException {

    this.originalJson = json.deepCopy();

    json = json.findPath("data");

    aggDefId = json.findPath("agg_def_id").intValue();

    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    int combId = json.findPath("agg_comb_id").intValue();
    comb = AggCombDao.findById(combId);

    if (!json.findPath("aux_agg_comb_id").isMissingNode()) {
      auxComb = AggCombDao.findById(json.findPath("aux_agg_comb_id").intValue());
      auxOp = AuxiliaryOp.valueOf(json.findPath("aux_op").textValue().toUpperCase());
    }

    try {
      this.rules = mapper.convertValue(json.findPath("rules"),
          TypeFactory.defaultInstance().constructCollectionType(List.class, DqRule.class));
    } catch (IllegalArgumentException ilaE) {
      ilaE.printStackTrace();
      throw new IllegalArgumentException("Invalid rules attribute: " + ilaE.getMessage());
    }


    if (!(json.findPath("name").isMissingNode())) {
      String n = json.findPath("name").textValue().trim();
      if (!n.isEmpty()) {
        setName(n);
      }
    }

    if (getName() == null || getName().isEmpty()) {
      setName(generatedName());
    }
  }

  public String generatedName() {
    if (auxOp != null) {
      StringBuilder sb = new StringBuilder("");
      switch (auxOp) {
        case DIFF:
          sb.append("Difference between ");
          break;
        case RATIO:
          sb.append("Ratio between ");
          break;
      }
      sb.append(comb.generatedName());
      sb.append(" and ");
      sb.append(auxComb.generatedName());
      return sb.toString();
    } else {
      return comb.generatedName();
    }
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public AggComb getComb() {
    return comb;
  }

  public AggComb getAuxComb() {
    return auxComb;
  }

  public AuxiliaryOp getAuxOp() {
    return auxOp;
  }

  public List<DqRule> getRules() {
    return rules;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAggDefId() {
    return aggDefId;
  }

  public JsonNode getOriginalJson() {
    return originalJson;
  }


}
