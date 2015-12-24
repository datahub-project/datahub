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
package dataquality.dao;

import dataquality.models.AggComb;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.BeanPropertyRowMapper;


/**
 * Created by zechen on 8/13/15.
 */
public class AggCombDao extends AbstractDao {


  private static final String FIND_BY_ID = "select * from dq_agg_comb where id = :id";
  private static final String FIND_BY_AGG_DEF_ID = "select * from dq_agg_comb where agg_def_id = :agg_def_id";


  public static AggComb findById(int id) {
    Map<String, Object> params = new HashMap<>();
    params.put("id", id);
    return dqNamedJdbc.queryForObject(FIND_BY_ID, params, new BeanPropertyRowMapper<>(AggComb.class));
  }

  public static List<AggComb> findByAggDefId(int aggDefId) {
    Map<String, Object> params = new HashMap<>();
    params.put("agg_def_id", aggDefId);
    return dqNamedJdbc.query(FIND_BY_AGG_DEF_ID, params, new BeanPropertyRowMapper<>(AggComb.class));
  }
}
