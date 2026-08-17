import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcards.module.scss";
import FilterCard from "../FilterCard";

// `aside` is optional supplementary content shown in a sticky rail to the right
// of the cards. It deliberately sits alongside the grid rather than above it:
// anything placed between the filter bar and the cards pushes the results
// themselves below the fold.
const FilterCards = ({ content, filterBar, aside }) =>
  content?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className={clsx(aside && styles.withAside)}>
          <div className="row">
            {content.map((props, idx) => (
              <FilterCard key={idx} {...props} />
            ))}
          </div>
          {aside}
        </div>
      </div>
    </div>
  ) : null;

export default FilterCards;
