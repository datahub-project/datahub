import React, { useState } from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import filterTagIndexes from "../../../filterTagIndexes.json";

// ─── Design tokens (match frontend) ──────────────────────────────────────────
const COLORS = {
  white: "#FFFFFF",
  border: "#EBECF0",
  primary: "#533FD1",
  primaryLight: "#EFF1FD",
  titleText: "#374066",
  descText: "#5F6685",
  placeholderText: "#8088A3",
  badgeBg: "#EBECF0",
  badgeText: "#5F6685",
  shadow: "0px 1px 2px 0px rgba(33, 23, 95, 0.07)",
};

const CARD_HEIGHT = 94;

// ─── Canonical category order ─────────────────────────────────────────────────
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
  "DataHub Tools",
];

const DEFAULT_METADATA = filterTagIndexes.ingestionSources;

// ─── Source card ─────────────────────────────────────────────────────────────
function SourceCard({ source }) {
  const [hovered, setHovered] = useState(false);
  const imgSrc = useBaseUrl(source.imgPath);

  return (
    <Link
      to={`/${source.Path}`}
      style={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        gap: 12,
        border: `1px solid ${hovered ? COLORS.primary : COLORS.border}`,
        borderRadius: 12,
        padding: "12px 16px",
        boxShadow: COLORS.shadow,
        backgroundColor: COLORS.white,
        height: CARD_HEIGHT,
        textDecoration: "none",
        color: "inherit",
        overflow: "hidden",
        boxSizing: "border-box",
        transition: "border-color 0.15s ease",
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
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
        <div
          style={{
            fontSize: 14,
            fontWeight: 700,
            color: COLORS.titleText,
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {source.Title}
        </div>
        <div
          style={{
            fontSize: 12,
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

// ─── Section header ───────────────────────────────────────────────────────────
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

// ─── Category filter pill ─────────────────────────────────────────────────────
function CategoryPill({ label, active, onClick }) {
  const [hovered, setHovered] = useState(false);

  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: "4px 12px",
        borderRadius: 200,
        border: `1px solid ${active ? COLORS.primary : COLORS.border}`,
        backgroundColor: active
          ? COLORS.primary
          : hovered
          ? COLORS.primaryLight
          : COLORS.white,
        color: active ? COLORS.white : COLORS.descText,
        fontSize: 13,
        fontWeight: active ? 600 : 400,
        cursor: "pointer",
        whiteSpace: "nowrap",
        transition: "all 0.15s ease",
        outline: "none",
        flexShrink: 0,
      }}
    >
      {label}
    </button>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────
export default function IntegrationsEmbed({ metadata = DEFAULT_METADATA }) {
  const [searchText, setSearchText] = useState("");
  const [activeCategories, setActiveCategories] = useState(new Set());

  // Get all categories present in the data, in canonical order
  const availableCategories = PLATFORM_TYPE_ORDER.filter((cat) =>
    metadata.some((s) => s.tags["Platform Type"] === cat)
  );

  const toggleCategory = (cat) => {
    setActiveCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) {
        next.delete(cat);
      } else {
        next.add(cat);
      }
      return next;
    });
  };

  // Filter sources
  const filtered = metadata.filter((source) => {
    const type = source.tags["Platform Type"] || "";
    if (activeCategories.size > 0 && !activeCategories.has(type)) return false;
    if (searchText) {
      const q = searchText.toLowerCase();
      return (
        source.Title.toLowerCase().includes(q) ||
        source.Description.toLowerCase().includes(q)
      );
    }
    return true;
  });

  // Group by Platform Type in canonical order, sorted alphabetically within
  const grouped = {};
  filtered.forEach((source) => {
    const type = source.tags["Platform Type"] || "Miscellaneous";
    if (!grouped[type]) grouped[type] = [];
    grouped[type].push(source);
  });

  const orderedGroups = PLATFORM_TYPE_ORDER.filter(
    (t) => grouped[t]?.length > 0
  ).map((t) => [
    t,
    [...grouped[t]].sort((a, b) => a.Title.localeCompare(b.Title)),
  ]);

  const unknownGroups = Object.keys(grouped)
    .filter((t) => !new Set(PLATFORM_TYPE_ORDER).has(t))
    .sort()
    .map((t) => [t, grouped[t]]);

  const allGroups = [...orderedGroups, ...unknownGroups];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {/* Search bar */}
      <div style={{ position: "relative", maxWidth: "100%" }}>
        <span
          style={{
            position: "absolute",
            left: 12,
            top: "50%",
            transform: "translateY(-50%)",
            color: COLORS.placeholderText,
            pointerEvents: "none",
            display: "flex",
            alignItems: "center",
          }}
        >
          {/* Magnifying glass SVG */}
          <svg width="16" height="16" viewBox="0 0 20 20" fill="none">
            <path
              d="M14.386 14.386l4.088 4.088-4.088-4.088c-2.942 2.942-7.711 2.942-10.653 0-2.942-2.942-2.942-7.712 0-10.654 2.942-2.941 7.711-2.941 10.653 0 2.942 2.942 2.942 7.712 0 10.654z"
              stroke="currentColor"
              strokeWidth="1.5"
              fill="none"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>
        <input
          type="text"
          placeholder="Search integrations..."
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          style={{
            width: "100%",
            height: 40,
            paddingLeft: 36,
            paddingRight: 16,
            borderRadius: 8,
            border: `1px solid ${COLORS.border}`,
            boxShadow: COLORS.shadow,
            fontSize: 14,
            color: COLORS.titleText,
            outline: "none",
            boxSizing: "border-box",
            backgroundColor: COLORS.white,
          }}
          onFocus={(e) => {
            e.target.style.borderColor = COLORS.primary;
            e.target.style.boxShadow = `0px 0px 0px 2px #DDD9F7`;
          }}
          onBlur={(e) => {
            e.target.style.borderColor = COLORS.border;
            e.target.style.boxShadow = COLORS.shadow;
          }}
        />
      </div>

      {/* Horizontal category filter pills */}
      <div
        style={{
          display: "flex",
          flexDirection: "row",
          gap: 8,
          overflowX: "auto",
          paddingBottom: 2,
          msOverflowStyle: "none",
          scrollbarWidth: "none",
        }}
      >
        {availableCategories.map((cat) => (
          <CategoryPill
            key={cat}
            label={cat}
            active={activeCategories.has(cat)}
            onClick={() => toggleCategory(cat)}
          />
        ))}
      </div>

      {/* Grouped card sections */}
      {allGroups.length > 0 ? (
        allGroups.map(([category, sources]) => (
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
