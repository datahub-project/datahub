/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef } from "react";


import clsx from "clsx";
import  { translate } from "@docusaurus/Translate";
import styles from "./search.module.scss";
import { useSearchPage } from "@docusaurus/theme-common/internal";


function FilterBar({textState, setTextState}) {
  return (
    <div className="DocSearch row">
      <div className="col col--offset-3 col--6">
        <form onSubmit={(e) => e.preventDefault()} className={styles.searchForm}>
          <input
            type="search"
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
          <svg width="20" height="20" className={clsx("DocSearch-Search-Icon", styles.searchIcon)} viewBox="0 0 20 20">
            <path
              d="M14.386 14.386l4.0877 4.0877-4.0877-4.0877c-2.9418 2.9419-7.7115 2.9419-10.6533 0-2.9419-2.9418-2.9419-7.7115 0-10.6533 2.9418-2.9419 7.7115-2.9419 10.6533 0 2.9419 2.9418 2.9419 7.7115 0 10.6533z"
              stroke="currentColor"
              fill="none"
              fillRule="evenodd"
              strokeLinecap="round"
              strokeLinejoin="round"
            ></path>
          </svg>
        </form>
    
      </div>
    </div>
  );
}

export default FilterBar;
