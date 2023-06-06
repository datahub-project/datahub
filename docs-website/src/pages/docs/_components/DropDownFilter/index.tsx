/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef } from "react";
import { Row, Col, Checkbox } from "antd";
import type { SelectProps } from "antd";
import { Select } from "antd";

function DropDownFilter({ filterState, setFilterState, filterOptions }) {
  function SingleFilter({
    filterState,
    setFilterState,
    filter,
    filterOptions,
  }) {
    const toArray = [...filterOptions[filter]];
    const [selectedItems, setSelectedItems] = useState<any[]>([]);
    const options: SelectProps["options"] = [];

    toArray.forEach((item) => {
      if (!filterState.includes(item)) {
        options.push({
          label: item,
          value: item,
        });
      }
    });

    const toggleFilter = (item) => {
      if (filterState.includes(item)) {
        setFilterState(filterState.filter((val) => val !== item));
      } else {
        setFilterState([...filterState, item]);
      }
    };
    const handleChange = (values: string[]) => {
      values.forEach((value) => {
        toggleFilter(value);
      });
      filterState
        .filter((val) => {
          return toArray.includes(val);
        })
        .forEach((value) => {
          if (!values.includes(value)) {
            toggleFilter(value);
          }
        });
    };

    let returnElement = <div />;
    if (toArray.length <= 2) {
      returnElement = (
        <Row
          style={{
            width: "auto",
            display: "flex",
            flex: "1 1 auto",
            flexDirection: "row",
            justifyContent: "space-between",
          }}
        >
          <Col style={{ marginRight: "10px" }}>{filter}</Col>
          <Col
            style={{ width: "55%", display: "flex", justifyContent: "start" }}
          >
            {toArray.length > 0 &&
              toArray.map((item) => {
                return (
                  <Checkbox
                    key={item}
                    onChange={(e) => {
                      toggleFilter(item);
                    }}
                    checked={filterState.includes(item)}
                  >
                    {item}
                  </Checkbox>
                );
              })}
          </Col>
        </Row>
      );
    } else {
      returnElement = (
        <Row
          style={{
            width: "auto",
            display: "flex",
            flex: "1 1 auto",
            flexDirection: "row",
            justifyContent: "space-between",
          }}
        >
          <Col>{filter}</Col>
          <Select
            mode="multiple"
            allowClear
            value={filterState.filter((val) => {
              return toArray.includes(val);
            })}
            bordered={true}
            style={{ width: "55%", marginLeft: "10px" }}
            placeholder="Select"
            onChange={handleChange}
            options={options}
          />
        </Row>
      );
    }
    return (
      <Row
        style={{
          display: "flex",
          justifyContent: "center",
          width: "auto",
          flex: "1 1 auto",
          marginTop: "16px",
        }}
      >
        {returnElement}
      </Row>
    );
  }

  const keys = Object.keys(filterOptions);
  var width: any = keys.length > 1 ? 100 / keys.length : 100;
  width = width + "%";
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "center",
        width: "auto",
        flexDirection: "column",
      }}
    >
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
              key={filter}
            />
          );
        })}
    </div>
  );
}

export default DropDownFilter;
