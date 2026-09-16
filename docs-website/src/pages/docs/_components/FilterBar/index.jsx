/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef, useCallback } from "react";

import clsx from "clsx";
import { translate } from "@docusaurus/Translate";
import styles from "./search.module.scss";
import DropDownFilter from "../../_components/DropDownFilter";
import { FilterFilled } from "@ant-design/icons";
import { Tag, Switch } from "antd";

function FilterBar({
  textState,
  setTextState,
  filterState,
  setFilterState,
  filterOptions,
  allowExclusivity,
  setIsExclusive,
  categoryCounts,
}) {
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [selectedFilters, setSelectedFilters] = useState([]);
  const [isSelectedExclusive, setIsSelectedExclusive] = useState(false);
  const searchTimerRef = useRef(null);

  const trackSearch = useCallback((query) => {
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => {
      if (query.trim().length >= 2) {
        window.gtag?.("event", "integration_search", {
          search_term: query.trim().toLowerCase(),
        });
      }
    }, 800);
  }, []);
  function toggleFilters() {
    setFiltersOpen(!filtersOpen);
  }
  function toggleSelectedExclusive() {
    setIsSelectedExclusive(!isSelectedExclusive);
  }

  function applyFilters() {
    window.gtag?.("event", "integration_filter_apply", {
      filters: selectedFilters.join(", "),
      filter_count: selectedFilters.length,
      match_mode: isSelectedExclusive ? "all" : "any",
    });
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

  function toggleCategoryPill(category) {
    const isRemoving = filterState.includes(category);
    window.gtag?.("event", "integration_category_click", {
      category_name: category,
      action: isRemoving ? "remove" : "add",
    });
    if (isRemoving) {
      removeFilter(category);
    } else {
      const next = [...filterState, category];
      setSelectedFilters(next);
      setFilterState(next);
    }
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
              onChange={(e) => {
                setTextState(e.target.value);
                trackSearch(e.target.value);
              }}
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
          {categoryCounts &&
            Object.keys(categoryCounts).length > 0 &&
            !filtersOpen && (
              <div className={styles.categoryPills}>
                {Object.entries(categoryCounts)
                  .sort((a, b) => b[1] - a[1])
                  .map(([category, count]) => (
                    <button
                      key={category}
                      className={`${styles.categoryPill} ${
                        filterState.includes(category)
                          ? styles.categoryPillActive
                          : ""
                      }`}
                      onClick={() => toggleCategoryPill(category)}
                    >
                      {category}
                      <span className={styles.categoryPillCount}>
                        {count}
                      </span>
                    </button>
                  ))}
              </div>
            )}
          {filtersOpen && (
            <div className={styles.filterPanel}>
              <div className={styles.filterPanelHeader}>
                <span className={styles.filterPanelTitle}>Advanced Filters</span>
                <button
                  onClick={toggleFilters}
                  className={styles.filterPanelClose}
                  aria-label="Close filters"
                >
                  ✕
                </button>
              </div>
              <DropDownFilter
                filterState={selectedFilters}
                setFilterState={setSelectedFilters}
                filterOptions={filterOptions}
                excludeKeys={categoryCounts ? ["Platform Type"] : []}
              />
              <div className={styles.filterPanelActions}>
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
                <div className={styles.filterPanelButtons}>
                  <button
                    onClick={removeFilters}
                    className={styles.filterPanelReset}
                  >
                    Reset
                  </button>
                  <button
                    onClick={applyFilters}
                    className={styles.filterPanelApply}
                  >
                    Apply Filters
                  </button>
                </div>
              </div>
            </div>
          )}
          {!filtersOpen && selectedFilters.length > 0 && (
            <div>
              {filterState
                .filter(
                  (filter) =>
                    !categoryCounts || !(filter in categoryCounts)
                )
                .map((filter, i) => (
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
