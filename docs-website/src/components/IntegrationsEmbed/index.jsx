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

// Height of the sticky search bar strip (input 38px + padding-bottom 16px)
const SEARCH_STRIP_HEIGHT = 54;

// Support status badge config
// TODO (Gabe): Review status values — Community tier needed; unify badge
// styling with generated source doc pages.
const STATUS_CONFIG = {
  CERTIFIED: { label: "Certified", bg: "#DCFCE7", color: "#15803D", border: "#86EFAC" },
  INCUBATING: { label: "Incubating", bg: "#DBEAFE", color: "#1D4ED8", border: "#93C5FD" },
  TESTING: { label: "Testing", bg: "#F1F5F9", color: "#64748B", border: "#CBD5E1" },
};

// Canonical order — must stay in sync with:
//   • Frontend Create Source flow  (ingestV2/.../utils.ts PRESORTED_CATEGORIES_*)
//   • Sidebar SIDEBAR_CATEGORY_ORDER  (docs-website/sidebars.js)
// Sources within each category are sorted alphabetically by docgen.py.
const CATEGORY_ORDER = [
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

// ─── Support status badge ─────────────────────────────────────────────────────
function StatusBadge({ status }) {
  const cfg = STATUS_CONFIG[status];
  if (!cfg) return null;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "1px 7px",
        borderRadius: 100,
        fontSize: 10,
        fontWeight: 500,
        background: cfg.bg,
        color: cfg.color,
        border: `1px solid ${cfg.border}`,
        lineHeight: 1.5,
        whiteSpace: "nowrap",
        flexShrink: 0,
      }}
    >
      {cfg.label}
    </span>
  );
}

// ─── Source card ──────────────────────────────────────────────────────────────
// Layout: [Logo] | [Name (bold), Badge below] — no description copy.
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
        gap: 10,
        border: `1px solid ${hovered ? C.primary : C.border}`,
        borderRadius: 10,
        padding: "9px 12px",
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
      {/* Logo */}
      <img
        src={imgSrc}
        alt={source.Title}
        style={{
          width: 24,
          height: 24,
          maxWidth: 24,
          objectFit: "contain",
          flexShrink: 0,
        }}
        onError={(e) => { e.target.style.display = "none"; }}
      />

      {/* Name + badge stacked */}
      <div style={{ display: "flex", flexDirection: "column", gap: 3, minWidth: 0, flex: 1 }}>
        <span
          style={{
            fontSize: 12,
            fontWeight: 700,
            color: C.titleText,
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {source.Title}
        </span>
        {source.support_status && <StatusBadge status={source.support_status} />}
      </div>
    </Link>
  );
}

// ─── Section header (sticky below search bar) ─────────────────────────────────
function SectionHeader({ label, count }) {
  return (
    <div
      style={{
        position: "sticky",
        // Stick below the sticky search bar
        top: `calc(var(--ifm-navbar-height, 60px) + ${SEARCH_STRIP_HEIGHT}px)`,
        zIndex: 9,
        backgroundColor: C.white,
        paddingTop: 10,
        paddingBottom: 10,
        borderBottom: `1px solid ${C.border}`,
        display: "flex",
        alignItems: "center",
        gap: 8,
        fontSize: 15,
        fontWeight: 700,
        color: C.titleText,
      }}
    >
      {label}
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
        {count}
      </span>
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────
export default function IntegrationsEmbed({ metadata = DEFAULT_METADATA }) {
  const [searchText, setSearchText] = useState("");

  const filtered = metadata.filter((source) => {
    if (!searchText) return true;
    const q = searchText.toLowerCase();
    return (
      source.Title.toLowerCase().includes(q) ||
      source.Description.toLowerCase().includes(q)
    );
  });

  // Group by Platform Type in canonical order; sort alphabetically within
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

  return (
    <div style={{ display: "flex", flexDirection: "column" }}>
      {/* Sticky search bar */}
      <div
        style={{
          position: "sticky",
          top: "var(--ifm-navbar-height, 60px)",
          zIndex: 10,
          backgroundColor: C.white,
          paddingBottom: 16,
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

      {/* Grouped sections */}
      {orderedGroups.length > 0 ? (
        orderedGroups.map(([category, sources]) => (
          <div key={category} style={{ marginBottom: 24 }}>
            <SectionHeader label={category} count={sources.length} />
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(170px, 1fr))",
                gap: 6,
                paddingTop: 10,
              }}
            >
              {sources.map((source) => (
                <SourceCard key={source.Path} source={source} />
              ))}
            </div>
          </div>
        ))
      ) : (
        <p style={{ color: C.descText }}>No integrations found.</p>
      )}
    </div>
  );
}
