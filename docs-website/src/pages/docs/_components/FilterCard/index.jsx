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
  isApiConnector,
  requestNativeUrl,
}) => {
  function handleCardClick() {
    window.gtag?.("event", "integration_card_click", {
      integration_name: title,
      integration_path: to,
      connection_type: isApiConnector ? "api" : "native",
      platform_type: filters?.["Platform Type"] || "",
    });
  }

  function handleRequestNativeClick(e) {
    e.preventDefault();
    e.stopPropagation();
    window.gtag?.("event", "native_connector_request", {
      integration_name: title,
    });
    window.open(requestNativeUrl, "_blank", "noopener,noreferrer");
  }

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

  const supportStatus = filters?.["Support Status"] || "";

  function getSupportBadgeClass() {
    const status = supportStatus.trim().toLowerCase();
    if (status === "certified") return styles.badgeCertified;
    if (status === "incubating") return styles.badgeIncubating;
    if (status === "testing") return styles.badgeTesting;
    return "";
  }

  return (
    <div className={clsx("col col--4", styles.featureCol)}>
      <Link
        to={useBaseUrl(to)}
        className={clsx("card", styles.feature)}
        onClick={handleCardClick}
      >
        {supportStatus && (
          <div className={clsx(styles.supportBadge, getSupportBadgeClass())}>
            {supportStatus.trim() === "Certified" && "✓ "}
            {supportStatus.trim()}
          </div>
        )}
        <div className={clsx("card__header", styles.featureHeader)}>
          {image && (
            <div className={styles.featureImage}>
              <img src={useBaseUrl(image)} />
            </div>
          )}
          <h2>
            {title}
            {isApiConnector && (
              <Tag
                color="blue"
                style={{
                  marginLeft: "0.5rem",
                  fontSize: "0.7rem",
                  verticalAlign: "middle",
                }}
              >
                API
              </Tag>
            )}
          </h2>
        </div>
        <hr />
        <div className="card__body">
          <div>{description}</div>
        </div>
        {(useTags || useFilters || (isApiConnector && requestNativeUrl)) && (
          <div className="card__footer">
            {useTags && renderTags()}
            {useFilters && renderFilters()}
            {isApiConnector && requestNativeUrl && (
              <div
                style={{
                  marginTop: "0.5rem",
                  fontSize: "0.8rem",
                }}
              >
                <a
                  href={requestNativeUrl}
                  onClick={handleRequestNativeClick}
                  style={{ fontWeight: 500 }}
                >
                  Request Native Connector &rarr;
                </a>
              </div>
            )}
          </div>
        )}
      </Link>
    </div>
  );
};

export default FilterCard;
