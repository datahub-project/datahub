/**
 * IntegrationsEmbed — Sources landing page component.
 *
 * TODO (Filters): Add filter pills for Ecosystem (Google, AWS, Azure) and
 *   Support Status once values are confirmed and badge styling is standardised.
 *
 * TODO (Support Status – Gabe): Review status values across all sources.
 *   A `Community` tier is needed. Badge styling should also be unified with
 *   the support-status badges rendered on individual generated source doc pages.
 *
 * TODO (Parity – Paulina & Chris): This page must always reflect the same
 *   source list, groupings, and ordering as the Create Source tab in the
 *   DataHub UI. Establish a shared source-of-truth so divergence is caught
 *   automatically (see ingestV2/source/builder/sources.json).
 */

import React, { useState } from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import filterTagIndexes from "../../../filterTagIndexes.json";

// ─── Design tokens ────────────────────────────────────────────────────────────
const C = {
  white: "#FFFFFF",
  border: "#EBECF0",
  primary: "#533FD1",
  titleText: "#374066",
  descText: "#5F6685",
  placeholder: "#8088A3",
  badgeBg: "#EBECF0",
  badgeText: "#5F6685",
  shadow: "0px 1px 2px 0px rgba(33, 23, 95, 0.07)",
};

// Support status badge config — styled to match the frontend Create Source
// pill variants (alchemy-components/components/Pills):
//   Certified  ↔ "New"      → color='blue',    variant='filled'
//   Incubating ↔ "Popular"  → color='primary',  variant='filled'
//   Testing    ↔ "External" → color='primary',  variant='outline'
// TODO (Gabe): Review values; Community tier needed; unify with source doc pages.
const STATUS_CONFIG = {
  CERTIFIED: {
    label: "Certified",
    bg: "#F1FBFE",     // blue[0]
    color: "#448EA9",  // blue[700]
    border: "transparent",
    outline: false,
  },
  INCUBATING: {
    label: "Incubating",
    bg: "#F1F3FD",     // primary[0] / violet[0]
    color: "#3B2D94",  // primary[700]
    border: "transparent",
    outline: false,
  },
  TESTING: {
    label: "Testing",
    bg: "transparent",
    color: "#3B2D94",  // primary[700]
    border: "#CAC3F1", // primary[100] — more visible than primary[0]
    outline: true,
  },
};

// Canonical order — keep in sync with the frontend Create Source flow and
// SIDEBAR_CATEGORY_ORDER in sidebars.js.
const CATEGORY_ORDER = [
  "Data Warehouse",
  "Data Lake",
  "BI & Analytics",
  "Database",
  "ETL / ELT",
  "Query Engine",
  "ML Platforms",
  "Orchestration",
  "Context Documents",
  "Miscellaneous",
  "DataHub Tools",
];

const DEFAULT_METADATA = filterTagIndexes.ingestionSources;

// ─── Chevron icons ────────────────────────────────────────────────────────────
function ChevronDown() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

function ChevronRight() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="9 18 15 12 9 6" />
    </svg>
  );
}

// ─── Status badge ─────────────────────────────────────────────────────────────
function StatusBadge({ status }) {
  const cfg = STATUS_CONFIG[status];
  if (!cfg) return null;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "2px 8px",
        borderRadius: 200,
        fontSize: 11,
        fontWeight: 500,
        background: cfg.bg,
        color: cfg.color,
        border: `1px solid ${cfg.outline ? cfg.border : "transparent"}`,
        lineHeight: 1.5,
        whiteSpace: "nowrap",
      }}
    >
      {cfg.label}
    </span>
  );
}

// ─── Source card ──────────────────────────────────────────────────────────────
// Layout: [logo container] [name]  — badge floats absolute top-right.
// Logo container uses DataHub's lightest violet tint (primary[0] #F1F3FD)
// with an 8px square radius — on-brand without mimicking OpenMetadata's circles.
function SourceCard({ source }) {
  const [hovered, setHovered] = useState(false);
  const imgSrc = useBaseUrl(source.imgPath);

  return (
    <Link
      to={`/${source.Path}`}
      style={{
        position: "relative",
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        gap: 10,
        border: `1px solid ${hovered ? C.primary : C.border}`,
        borderRadius: 12,
        padding: "12px",
        boxShadow: C.shadow,
        backgroundColor: C.white,
        textDecoration: "none",
        color: "inherit",
        overflow: "hidden",
        boxSizing: "border-box",
        transition: "border-color 0.15s ease",
        minWidth: 0,
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      {/* Logo container — light violet tint, square radius */}
      <div
        style={{
          width: 36,
          height: 36,
          borderRadius: 8,
          backgroundColor: "#F8F9FB",
          border: "1px solid #EBECF0",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          flexShrink: 0,
        }}
      >
        <img
          src={imgSrc}
          alt={source.Title}
          style={{ width: 22, height: 22, objectFit: "contain" }}
          onError={(e) => { e.target.parentElement.style.backgroundColor = "#F1F3FD"; e.target.style.display = "none"; }}
        />
      </div>

      {/* Name — wraps to 2 lines so nothing is clipped with ellipsis */}
      <span
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: C.titleText,
          lineHeight: 1.35,
          display: "-webkit-box",
          WebkitLineClamp: 2,
          WebkitBoxOrient: "vertical",
          overflow: "hidden",
          flex: 1,
          minWidth: 0,
          paddingRight: source.support_status ? 52 : 0,
        }}
      >
        {source.Title}
      </span>

      {/* Badge — absolute top-right */}
      {source.support_status && (
        <div style={{ position: "absolute", top: 8, right: 8 }}>
          <StatusBadge status={source.support_status} />
        </div>
      )}
    </Link>
  );
}

// ─── Collapsible section ──────────────────────────────────────────────────────
function Section({ category, sources, isOpen, onToggle }) {
  const [hovered, setHovered] = useState(false);

  return (
    <div style={{ marginBottom: 16 }}>
      {/* Header row — clickable, toggles collapse */}
      <div
        onClick={onToggle}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          paddingTop: 10,
          paddingBottom: 10,
          borderBottom: `1px solid ${C.border}`,
          cursor: "pointer",
          userSelect: "none",
        }}
      >
        {/* Left: name + count badge */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 15, fontWeight: 700, color: C.titleText }}>
            {category}
          </span>
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              background: C.badgeBg,
              color: C.badgeText,
              borderRadius: 100,
              fontSize: 11,
              fontWeight: 500,
              padding: "0 6px",
              minWidth: 20,
              height: 18,
              lineHeight: 1,
            }}
          >
            {sources.length}
          </span>
        </div>

        {/* Right: chevron */}
        <span style={{ color: hovered ? C.primary : C.badgeText, display: "flex", transition: "color 0.15s" }}>
          {isOpen ? <ChevronDown /> : <ChevronRight />}
        </span>
      </div>

      {/* Card grid — hidden when collapsed */}
      {isOpen && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(190px, 1fr))",
            gap: 6,
            paddingTop: 10,
          }}
        >
          {sources.map((source) => (
            <SourceCard key={source.Path} source={source} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────
export default function IntegrationsEmbed({ metadata = DEFAULT_METADATA }) {
  const [searchText, setSearchText] = useState("");
  // All categories open by default
  const [openCategories, setOpenCategories] = useState(() => new Set(CATEGORY_ORDER));

  const toggleCategory = (cat) => {
    setOpenCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  const filtered = metadata.filter((source) => {
    if (!searchText) return true;
    const q = searchText.toLowerCase();
    return (
      source.Title.toLowerCase().includes(q) ||
      source.Description.toLowerCase().includes(q)
    );
  });

  const grouped = {};
  filtered.forEach((source) => {
    const type = source.tags["Platform Type"] || "Miscellaneous";
    if (!grouped[type]) grouped[type] = [];
    grouped[type].push(source);
  });

  const known = new Set(CATEGORY_ORDER);
  const orderedGroups = [
    ...CATEGORY_ORDER.filter((t) => grouped[t]?.length > 0).map((t) => [
      t,
      [...grouped[t]].sort((a, b) => a.Title.localeCompare(b.Title)),
    ]),
    ...Object.keys(grouped)
      .filter((t) => !known.has(t))
      .sort()
      .map((t) => [t, [...grouped[t]].sort((a, b) => a.Title.localeCompare(b.Title))]),
  ];

  // When searching, auto-expand all matching categories
  const effectiveOpen = searchText
    ? new Set(orderedGroups.map(([cat]) => cat))
    : openCategories;

  return (
    <div style={{ display: "flex", flexDirection: "column" }}>
      {/* Description */}
      <p style={{ fontSize: 14, color: C.descText, margin: "0 0 16px", lineHeight: 1.6 }}>
        Explore all the ways you can connect your data tools with DataHub.
      </p>

      {/* Search bar — sticky below navbar */}
      <div
        style={{
          position: "sticky",
          top: "var(--ifm-navbar-height, 60px)",
          zIndex: 10,
          backgroundColor: C.white,
          paddingBottom: 14,
          marginBottom: 4,
        }}
      >
        <div style={{ position: "relative", maxWidth: 460 }}>
          <span
            style={{
              position: "absolute",
              left: 10,
              top: "50%",
              transform: "translateY(-50%)",
              color: C.placeholder,
              pointerEvents: "none",
              display: "flex",
            }}
          >
            <svg width="14" height="14" viewBox="0 0 20 20" fill="none">
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
              height: 38,
              paddingLeft: 30,
              paddingRight: 12,
              borderRadius: 8,
              border: `1px solid ${C.border}`,
              boxShadow: C.shadow,
              fontSize: 13,
              color: C.titleText,
              outline: "none",
              boxSizing: "border-box",
              backgroundColor: C.white,
            }}
            onFocus={(e) => {
              e.target.style.borderColor = C.primary;
              e.target.style.boxShadow = "0px 0px 0px 2px #DDD9F7";
            }}
            onBlur={(e) => {
              e.target.style.borderColor = C.border;
              e.target.style.boxShadow = C.shadow;
            }}
          />
        </div>
      </div>

      {/* Collapsible sections */}
      {orderedGroups.length > 0 ? (
        orderedGroups.map(([category, sources]) => (
          <Section
            key={category}
            category={category}
            sources={sources}
            isOpen={effectiveOpen.has(category)}
            onToggle={() => toggleCategory(category)}
          />
        ))
      ) : (
        <p style={{ color: C.descText }}>No integrations found.</p>
      )}
    </div>
  );
}
