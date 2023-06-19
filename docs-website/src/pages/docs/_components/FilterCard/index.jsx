import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcard.module.scss";
import { Tag } from "antd";

const FilterCard = ({
  image,
  title,
  description,
  to,
  filters,
  tags,
  useTags,
  useFilters,
  filterState,
}) => {
  function renderFilters() {
    const keys = Object.keys(filters);

    return (
      <div
        style={{
          display: "flex",
          flex: "0 0 auto",
          height: "auto",
          width: "auto",
          flexDirection: "column",
          justifyContent: "end",
          alignItems: "start",
          fontSize: "0.65rem",
        }}
      >
        {keys.map((key) => {
          if (filters[key] === "") return;
          return (
            <div
              key={key}
              style={{
                display: "flex",
                flex: "0 0 auto",
                flexWrap: "wrap",
                flexDirection: "row",
              }}
            >
              {" "}
              {key + ": "}
              {filters[key].split(",").map((tag, i) => (
                <Tag
                  key={tag + i}
                  value={tag}
                  style={{
                    marginTop: 0,
                    marginBottom: ".15rem",
                    marginLeft: ".15rem",
                    marginRight: 0,
                    fontSize: "0.65rem",
                  }}
                >
                  {tag}
                </Tag>
              ))}
            </div>
          );
        })}
      </div>
    );
  }

  function renderTags() {
    return (
      <div
        style={{
          display: "flex",
          flex: "0 0 auto",
          height: "auto",
          width: "auto",
          flexDirection: "row",
          flexWrap: "wrap",
          alignItems: "start",
          fontSize: "0.65rem",
        }}
      >
        {tags
          .filter((tag) => {
            return filterState.includes(tag);
          })
          .map((tag, i) => (
            <Tag
              key={tag + i}
              value={tag}
              style={{
                marginTop: 0,
                marginBottom: ".15rem",
                marginLeft: ".15rem",
                marginRight: 0,
                fontSize: "0.65rem",
              }}
            >
              {tag.trim()}
            </Tag>
          ))}
      </div>
    );
  }

  return (
    <div className={clsx("col col--4", styles.featureCol)}>
      <Link to={useBaseUrl(to)} className={clsx("card", styles.feature)}>
        <div className={clsx("card__header", styles.featureHeader)}>
          {image && (
            <div className={styles.featureImage}>
              <img src={useBaseUrl(image)} />
            </div>
          )}
          <h2>{title}</h2>
        </div>
        <hr />
        <div className="card__body">
          <div>{description}</div>
        </div>
        {(useTags || useFilters) && (
          <div className="card__footer">
            {useTags && renderTags()}
            {useFilters && renderFilters()}
          </div>
        )}
      </Link>
    </div>
  );
};

export default FilterCard;
