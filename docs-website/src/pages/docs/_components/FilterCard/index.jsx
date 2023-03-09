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
          justifyContent: "end",
          alignItems: "start",
          fontSize: "0.65rem",
        }}
      >
        {tags.map((tag, i) => (
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
    <div className="col col--4">
      <Link to={useBaseUrl(to)} className={clsx("card", styles.feature)}>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            justifyContent: useTags || useFilters ? "space-between" : "center",
          }}
        >
          <div
            style={{
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
            }}
          >
            {image && (
              <div style={{ display: "flex", alignItems: "center" }}>
                <img src={useBaseUrl(image)} />
              </div>
            )}
            <strong>{title}</strong>
          </div>

          <div>
            <span style={{ marginLeft: "1rem" }}>{description}</span>
          </div>
          {useTags && renderTags()}
          {useFilters && renderFilters()}
        </div>
      </Link>
    </div>
  );
};

export default FilterCard;
