/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef } from "react";


import type { SelectProps } from 'antd';
import { Select} from 'antd';



type selectedFilters = {
  Difficulty: ["easy", "medium", "hard"],
  PlatformType: ["datalake", "bitool", "orchestrator"],
  PushPull: ["push", "pull"]
}

function DropDownFilter({filterState, setFilterState, filterOptions}) {
 
  const  options: SelectProps['options'] = []
  for(const filter in filterOptions) {
    var tempOptions: SelectProps['options'] = []
    for(let x = 0; x< filterOptions[filter].length; x++) {
      if(!filterState.includes(filterOptions[filter][x])){
        tempOptions.push({
          label: filterOptions[filter][x],
          value: filterOptions[filter][x],
        })
      }
      
  }
  options.push({
    label: filter,
    options: tempOptions
  })
  }
  
  


  const handleChange = (values: string[]) => {
    console.log(`selected ${values}`);
    filterState = values;
    setFilterState(filterState);
  } 


  return (
    
  <Select
    mode="multiple"
    allowClear
    bordered={false}
    style={{ width: '30%' }}
    placeholder="Please select filters"
    onChange={handleChange}
    options={options}
  />
    )
}

export default DropDownFilter;
