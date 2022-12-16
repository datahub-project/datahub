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

function SingleFilter({filterState, setFilterState, filter, width, filterOptions}) {
  
  const  options: SelectProps['options'] = []
  for(let i = 0; i< filterOptions[filter].length; i++) {
    options.push({
      label: filterOptions[filter][i],
      value: filterOptions[filter][i],
    })
  }
  

  const handleChange = (values: string[]) => {
    console.log(`selected ${values}`);
    filterState[filter] = values;
    console.log("filterState",filterState[filter])
    setFilterState(selected => ({
      ...selected,
    }));
  } 


  return (
    
  <Select
    mode="multiple"
    allowClear
    bordered={false}
    style={{ width: width }}
    placeholder="Please select filters"
    onChange={handleChange}
    options={options}
  />
    )
  }
function DropDownFilter({filterState, setFilterState, filterOptions}) {

const keys = Object.keys(filterState);
var width: any = keys.length > 1 ? 100/keys.length : 100;
width = width + '%';

  return (
    <>
    {
      
keys.map((filter) => {
  
  return <SingleFilter filterState={filterState} setFilterState={setFilterState} filterOptions={filterOptions} filter={filter} width={width} key={filter} />
})

    }</>
  );
}

export default DropDownFilter;
