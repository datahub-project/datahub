import React, { useState } from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import FilterBar from "../../pages/docs/_components/FilterBar";
import filterTagIndexes from "../../../filterTagIndexes.json";

const DEFAULT_METADATA = filterTagIndexes.ingestionSources;

// ─── Design tokens (match frontend Create Source UI) ─────────────────────────
const COLORS = {
  white: "#FFFFFF",
  border: "#EBECF0",
  primary: "#533FD1",
  titleText: "#374066",
  descText: "#5F6685",
  badgeBg: "#EBECF0",
  badgeText: "#5F6685",
  pillBg: "#EFF1FD",
  pillText: "#533FD1",
};

const CARD_HEIGHT = 94;
const CARD_BORDER_RADIUS = 12;
const CARD_PADDING = "12px 16px";
const CARD_SHADOW = "0px 1px 2px 0px rgba(33, 23, 95, 0.07)";

// ─── Canonical Platform Type order ───────────────────────────────────────────
const PLATFORM_TYPE_ORDER = [
  "Data Warehouse",
  "Data Lake",
  "BI & Analytics",
  "Database",
  "ETL / ELT",
  "Query Engine",
  "ML Platforms",
  "Orchestration",
  "Context Document Sources",
  "Miscellaneous",
  "DataHub Internal",
];

function sortPlatformTypeOptions(options) {
  return new Set(
    [...options].sort((a, b) => {
      const ai = PLATFORM_TYPE_ORDER.indexOf(a);
      const bi = PLATFORM_TYPE_ORDER.indexOf(b);
      const aIdx = ai === -1 ? PLATFORM_TYPE_ORDER.length : ai;
      const bIdx = bi === -1 ? PLATFORM_TYPE_ORDER.length : bi;
      return aIdx !== bIdx ? aIdx - bIdx : a.localeCompare(b);
    })
  );
}

// ─── Individual source card ───────────────────────────────────────────────────
function SourceCard({ source }) {
  const [hovered, setHovered] = useState(false);
  const imgSrc = useBaseUrl(source.imgPath);
  const href = `/${source.Path}`;

  return (
    <Link
      to={href}
      style={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        gap: 12,
        border: `1px solid ${hovered ? COLORS.primary : COLORS.border}`,
        borderRadius: CARD_BORDER_RADIUS,
        padding: CARD_PADDING,
        boxShadow: CARD_SHADOW,
        backgroundColor: COLORS.white,
        height: CARD_HEIGHT,
        cursor: "pointer",
        textDecoration: "none",
        color: "inherit",
        overflow: "hidden",
        boxSizing: "border-box",
        transition: "border-color 0.15s ease",
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      {/* Logo */}
      <img
        src={imgSrc}
        alt={source.Title}
        style={{
          width: 32,
          height: 32,
          maxWidth: 32,
          objectFit: "contain",
          flexShrink: 0,
          alignSelf: "flex-start",
          marginTop: 4,
        }}
        onError={(e) => {
          e.target.style.display = "none";
        }}
      />

      {/* Text content */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 3,
          overflow: "hidden",
          flex: 1,
          minWidth: 0,
        }}
      >
        {/* Title row */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            fontSize: 14,
            fontWeight: 700,
            color: COLORS.titleText,
            whiteSpace: "nowrap",
            overflow: "hidden",
          }}
        >
          <span
            style={{
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {source.Title}
          </span>
        </div>

        {/* Description */}
        <div
          style={{
            fontSize: 12,
            fontWeight: 400,
            color: COLORS.descText,
            display: "-webkit-box",
            WebkitLineClamp: 2,
            WebkitBoxOrient: "vertical",
            overflow: "hidden",
            lineHeight: 1.45,
          }}
        >
          {source.Description}
        </div>
      </div>
    </Link>
  );
}

// ─── Section header (category name + count badge) ────────────────────────────
function SectionHeader({ label, count }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        fontSize: 16,
        fontWeight: 700,
        color: COLORS.titleText,
      }}
    >
      {label}
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          background: COLORS.badgeBg,
          color: COLORS.badgeText,
          borderRadius: 100,
          fontSize: 12,
          fontWeight: 500,
          padding: "0 7px",
          minWidth: 22,
          height: 20,
          lineHeight: 1,
        }}
      >
        {count}
      </span>
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────
export default function IntegrationsEmbed({ metadata = DEFAULT_METADATA }) {
  const [textState, setTextState] = useState("");
  const [filterState, setFilterState] = useState([]);
  const [isExclusive, setIsExclusive] = useState(false);

  // Build filter options for the FilterBar
  let filterOptions = {};
  metadata.forEach((data) => {
    Object.keys(data.tags).forEach((key) => {
      if (!filterOptions[key]) filterOptions[key] = new Set();
      data.tags[key].split(",").forEach((tag) => {
        const t = tag.trim();
        if (t) filterOptions[key].add(t);
      });
    });
  });
  if (filterOptions["Platform Type"]) {
    filterOptions["Platform Type"] = sortPlatformTypeOptions(
      filterOptions["Platform Type"]
    );
  }

  // Filter sources
  const filtered = metadata.filter((source) => {
    const allTags = Object.values(source.tags)
      .flatMap((v) => v.split(",").map((t) => t.trim()))
      .filter(Boolean);

    if (filterState.length > 0) {
      let flag = isExclusive;
      filterState.forEach((f) => {
        flag =
          (!isExclusive && (flag || allTags.includes(f))) ||
          (isExclusive && flag && allTags.includes(f));
      });
      if (!flag) return false;
    }

    if (textState) {
      const q = textState.toLowerCase();
      return (
        source.Title.toLowerCase().includes(q) ||
        source.Description.toLowerCase().includes(q)
      );
    }

    return true;
  });

  // Group by Platform Type in canonical order
  const grouped = {};
  filtered.forEach((source) => {
    const type = source.tags["Platform Type"] || "Miscellaneous";
    if (!grouped[type]) grouped[type] = [];
    grouped[type].push(source);
  });

  const orderedGroups = PLATFORM_TYPE_ORDER.filter(
    (t) => grouped[t]?.length > 0
  ).map((t) => [t, grouped[t]]);

  // Any ungrouped (unknown Platform Type)
  const knownTypes = new Set(PLATFORM_TYPE_ORDER);
  Object.keys(grouped)
    .filter((t) => !knownTypes.has(t))
    .sort()
    .forEach((t) => orderedGroups.push([t, grouped[t]]));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Search + filter bar */}
      <FilterBar
        textState={textState}
        setTextState={setTextState}
        filterState={filterState}
        setFilterState={setFilterState}
        filterOptions={filterOptions}
        allowExclusivity={false}
        setIsExclusive={setIsExclusive}
      />

      {/* Grouped card sections */}
      {orderedGroups.length > 0 ? (
        orderedGroups.map(([category, sources]) => (
          <div
            key={category}
            style={{ display: "flex", flexDirection: "column", gap: 12 }}
          >
            <SectionHeader label={category} count={sources.length} />
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
                gap: 8,
              }}
            >
              {sources.map((source) => (
                <SourceCard key={source.Path} source={source} />
              ))}
            </div>
          </div>
        ))
      ) : (
        <p style={{ color: COLORS.descText }}>No integrations found.</p>
      )}
    </div>
  );
}
