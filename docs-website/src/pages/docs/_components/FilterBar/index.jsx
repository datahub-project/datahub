/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef } from "react";

import clsx from "clsx";
import { translate } from "@docusaurus/Translate";
import styles from "./search.module.scss";
import DropDownFilter from "../../_components/DropDownFilter";
import { FilterFilled, CloseCircleFilled } from "@ant-design/icons";
import { Card, Button, Tag, Switch } from "antd";

function FilterBar({
  textState,
  setTextState,
  filterState,
  setFilterState,
  filterOptions,
  allowExclusivity,
  setIsExclusive,
}) {
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [selectedFilters, setSelectedFilters] = useState([]);
  const [isSelectedExclusive, setIsSelectedExclusive] = useState(false);
  function toggleFilters() {
    setFiltersOpen(!filtersOpen);
  }
  function toggleSelectedExclusive() {
    setIsSelectedExclusive(!isSelectedExclusive);
  }

  function applyFilters() {
    setFilterState(selectedFilters);
    setFiltersOpen(false);
    setIsExclusive(isSelectedExclusive);
  }
  function removeFilters() {
    setSelectedFilters([]);
    setFilterState([]);
    setIsExclusive(false);
    setIsSelectedExclusive(false);
  }
  function removeFilter(filter) {
    setSelectedFilters(selectedFilters.filter((f) => f !== filter));
    setFilterState(filterState.filter((f) => f !== filter));
  }

  return (
    <div>
      <div className="DocSearch row">
        <div className="col col--offset-3 col--6">
          <form
            onSubmit={(e) => e.preventDefault()}
            className={styles.searchForm}
          >
            <input
              type="text"
              name="q"
              className={styles.searchQueryInput}
              placeholder={translate({
                id: "theme.SearchPage.inputPlaceholder",
                message: "Filter Integrations",
                description: "The placeholder for search page input",
              })}
              aria-label={translate({
                id: "theme.SearchPage.inputLabel",
                message: "Filter",
                description: "The ARIA label for search page input",
              })}
              onChange={(e) => setTextState(e.target.value)}
              value={textState}
              autoComplete="off"
              autoFocus
            />

            <svg
              width="20"
              height="20"
              className={clsx("DocSearch-Search-Icon", styles.searchIcon)}
              viewBox="0 0 20 20"
            >
              <path
                d="M14.386 14.386l4.0877 4.0877-4.0877-4.0877c-2.9418 2.9419-7.7115 2.9419-10.6533 0-2.9419-2.9418-2.9419-7.7115 0-10.6533 2.9418-2.9419 7.7115-2.9419 10.6533 0 2.9419 2.9418 2.9419 7.7115 0 10.6533z"
                stroke="currentColor"
                fill="none"
                fillRule="evenodd"
                strokeLinecap="round"
                strokeLinejoin="round"
              ></path>
            </svg>
            <FilterFilled
              className={
                filtersOpen || filterState.length > 0
                  ? clsx("DocSearch-Filter-Icon", styles.filterIcon)
                  : clsx("DocSearch-Filter-Icon", styles.filterIconDark)
              }
              onClick={toggleFilters}
              fill={filtersOpen ? "blue" : "var(--docsearch-muted-color)"}
            />
          </form>
          {filtersOpen && (
            <Card
              style={{
                display: "flex",
                width: "auto",
                boxSizing: "border-box",
                boxShadow: "5px 8px 24px 5px rgba(208, 216, 243, 0.6)",
              }}
              bodyStyle={{
                display: "flex",
                justifyContent: "center",
                width: "100%",
                flexDirection: "column",
                padding: "1rem",
              }}
            >
              <CloseCircleFilled
                onClick={toggleFilters}
                className={clsx("DocSearch-Close-Icon", styles.closeIcon)}
              />{" "}
              <DropDownFilter
                filterState={selectedFilters}
                setFilterState={setSelectedFilters}
                filterOptions={filterOptions}
              />
              <div
                style={{
                  display: "flex",
                  width: "100%",
                  justifyContent: allowExclusivity
                    ? "space-between"
                    : "flex-end",
                  paddingTop: "1rem",
                }}
              >
                {allowExclusivity && (
                  <div>
                    <Switch
                      onChange={toggleSelectedExclusive}
                      checked={isSelectedExclusive}
                    />{" "}
                    {isSelectedExclusive
                      ? "Matches all tags "
                      : "Matches any tags "}
                  </div>
                )}
                <div>
                  <Button
                    onClick={removeFilters}
                    className={clsx(
                      "DocSearch-Reset-Button",
                      styles.resetButton
                    )}
                    style={{ marginRight: "1rem" }}
                  >
                    Reset
                  </Button>
                  <Button
                    onClick={applyFilters}
                    type="primary"
                    className={clsx(
                      "DocSearch-Filter-Button",
                      styles.filterButton
                    )}
                  >
                    Search
                  </Button>
                </div>
              </div>
            </Card>
          )}
          {!filtersOpen && selectedFilters.length > 0 && (
            <div>
              {filterState.map((filter, i) => (
                <Tag
                  closable
                  onClose={() => removeFilter(filter)}
                  value={filter}
                  key={filter + i}
                >
                  {filter}
                </Tag>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default FilterBar;
