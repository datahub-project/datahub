/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef } from "react";
import { Row, Col, Checkbox } from "antd";
import type { CheckboxChangeEvent } from "antd/es/checkbox";

function DropDownFilter({ filterState, setFilterState, filterOptions }) {
  function SingleFilter({
    filterState,
    setFilterState,
    filter,
    width,
    filterOptions,
  }) {
    const toggleFilter = (item) => {
      if (filterState.includes(item)) {
        setFilterState(filterState.filter((val) => val !== item));
      } else {
        setFilterState([...filterState, item]);
      }
    };

    const toArray = [...filterOptions[filter]];

    return (
      <Col style={{ marginLeft: 10 }}>
        <Row>{filter}</Row>

        {toArray.length > 0 &&
          toArray.map((item) => {
            return (
              <Row key={item}>
                <Checkbox
                  onChange={(e) => {
                    toggleFilter(item);
                  }}
                  checked={filterState.includes(item)}
                >
                  {item}
                </Checkbox>
              </Row>
            );
          })}
      </Col>
    );
  }

  const keys = Object.keys(filterOptions);
  var width: any = keys.length > 1 ? 100 / keys.length : 100;
  width = width + "%";
  return (
    <Row>
      {keys
        .sort((a, b) => {
          return filterOptions[a].size - filterOptions[b].size;
        })
        .map((filter) => {
          return (
            <SingleFilter
              filterState={filterState}
              setFilterState={setFilterState}
              filterOptions={filterOptions}
              filter={filter}
              width={width}
              key={filter}
            />
          );
        })}
    </Row>
  );
}

export default DropDownFilter;
