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
  Difficulty: {
    easy: Boolean,
    medium: Boolean,
    hard: Boolean
    },
  PlatformType: {
    datalake: Boolean,
    bitool: Boolean,
    orchestrator: Boolean
    },
  PushPull: {
    push: Boolean,
    pull: Boolean
  }
}


function SingleFilter({filterState, setFilterState, filter, width}) {
  
  const options: SelectProps['options'] = [];
  for (const option in filterState[filter]) {
    options.push({
      label: option,
      value: option,
    });
  }
  const handleChange = (values: string[]) => {
    console.log(`selected ${values}`);
    for (const option in filterState[filter]) {      
      if(values.indexOf(option) > -1)
        filterState[filter][option] = true;
      else
        filterState[filter][option] = false;
    }
    setFilterState(selected => ({
      ...selected,
    }));
  } 
  return (
    
  <Select
    mode="multiple"
    allowClear
    style={{ width: width }}
    placeholder="Please select"
    onChange={handleChange}
    options={options}
  />
    )
  }
function DropDownFilter({filterState, setFilterState}) {

const keys = Object.keys(filterState);
var width: any = keys.length > 1 ? 100/keys.length : 100;
width = width + '%';

  return (
    <>
    {
      
keys.map((filter) => {
  
  return <SingleFilter filterState={filterState} setFilterState={setFilterState} filter={filter} width={width} key={filter} />
})

    }</>
  );
}

export default DropDownFilter;
