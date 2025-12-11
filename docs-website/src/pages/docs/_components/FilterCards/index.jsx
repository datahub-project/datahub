/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcards.module.scss";
import FilterCard from "../FilterCard";

const FilterCards = ({ content, filterBar }) =>
  content?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row">
          {content.map((props, idx) => (
            <FilterCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default FilterCards;
