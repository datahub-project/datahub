import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcards.module.scss";
import FilterCard from "../FilterCard";

// `aside` is optional supplementary content shown in a sticky rail to the right
// of the cards at wide widths.
//
// It is deliberately rendered BEFORE the cards in the DOM. The two-column grid
// reorders it back to the right-hand column via `order` (see
// quicklinkcards.module.scss), so the visual result is unchanged above 1200px.
// Below that the grid collapses to one column and DOM order takes over, which
// puts the aside above the cards instead of stranding it below all ~148 of
// them - where a badge legend is functionally absent.
//
// The aside also survives an empty result set. It explains the badges rather
// than describing any particular card, so it stays useful when a filter matches
// nothing, and an empty state is shown in place of the grid.
const FilterCards = ({ content, filterBar, aside }) => {
  const hasContent = content?.length > 0;

  if (!hasContent && !aside) return null;

  return (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className={clsx(aside && styles.withAside)}>
          {aside}
          {hasContent ? (
            <div className="row">
              {content.map((props, idx) => (
                <FilterCard key={idx} {...props} />
              ))}
            </div>
          ) : (
            <p className={styles.noResults}>
              No connectors match the current filters.
            </p>
          )}
        </div>
      </div>
    </div>
  );
};

export default FilterCards;
