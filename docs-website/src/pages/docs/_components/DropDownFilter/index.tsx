/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useState } from "react";
import { Checkbox } from "antd";
import type { SelectProps } from "antd";
import { Select } from "antd";

function DropDownFilter({ filterState, setFilterState, filterOptions, excludeKeys = [] }) {
  function SingleFilter({
    filterState,
    setFilterState,
    filter,
    filterOptions,
  }) {
    const toArray = [...filterOptions[filter]];
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

    const labelStyle: React.CSSProperties = {
      fontSize: "0.85rem",
      fontWeight: 500,
      color: "var(--ifm-color-emphasis-700)",
      minWidth: "120px",
      paddingTop: "0.3rem",
    };

    const rowStyle: React.CSSProperties = {
      display: "flex",
      alignItems: "flex-start",
      gap: "1rem",
      width: "100%",
    };

    if (toArray.length <= 2) {
      return (
        <div style={{ ...rowStyle, alignItems: "center", padding: "0.5rem 0" }}>
          <span style={labelStyle}>{filter}</span>
          <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
            {toArray.map((item) => (
              <Checkbox
                key={item}
                onChange={() => toggleFilter(item)}
                checked={filterState.includes(item)}
              >
                {item}
              </Checkbox>
            ))}
          </div>
        </div>
      );
    }

    return (
      <div style={{ ...rowStyle, padding: "0.5rem 0" }}>
        <span style={labelStyle}>{filter}</span>
        <Select
          mode="multiple"
          allowClear
          value={filterState.filter((val) => toArray.includes(val))}
          style={{
            flex: 1,
            minWidth: 0,
          }}
          placeholder="Select..."
          onChange={handleChange}
          options={options}
        />
      </div>
    );
  }

  const keys = Object.keys(filterOptions);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
      {keys
        .filter((key) => !excludeKeys.includes(key))
        .sort((a, b) => filterOptions[a].size - filterOptions[b].size)
        .map((filter) => (
          <SingleFilter
            filterState={filterState}
            setFilterState={setFilterState}
            filterOptions={filterOptions}
            filter={filter}
            key={filter}
          />
        ))}
    </div>
  );
}

export default DropDownFilter;
