/**
 * IntegrationsEmbed — Sources landing page component.
 *
 * TODO (Filters): Add Platform Type filter pills for Ecosystem (Google, AWS, Azure)
 *   and Support Status once we have cleaner groupings and the support status
 *   values have been reviewed with Gabe (Community value needed, badge styling
 *   to be standardised across all source docs pages).
 *
 * TODO (Parity): Review with Paulina & Chris — this page must always reflect
 *   the exact same source list, groupings, and ordering as the Create Source
 *   tab in the DataHub UI. Establish a shared source-of-truth so divergence
 *   is caught automatically.
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

// Support status badge config
// TODO (Gabe): Review these values — a `Community` tier is needed, and badge
// styling should be unified across the generated source doc pages too.
const STATUS_CONFIG = {
  CERTIFIED: {
    label: "Certified",
    bg: "#DCFCE7",
    color: "#15803D",
    border: "#86EFAC",
  },
  INCUBATING: {
    label: "Incubating",
    bg: "#DBEAFE",
    color: "#1D4ED8",
    border: "#93C5FD",
  },
  TESTING: {
    label: "Testing",
    bg: "#F1F5F9",
    color: "#64748B",
    border: "#CBD5E1",
  },
};

// ─── Canonical category order — must stay in sync with the frontend
// Create Source flow (ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils.ts).
// Sources within each category are sorted alphabetically by the docgen.py
// pipeline (generate_source_category_manifest) and by this component's
// localeCompare sort. Do not reorder items here manually.
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
        padding: "2px 8px",
        borderRadius: 100,
        fontSize: 11,
        fontWeight: 500,
        background: cfg.bg,
        color: cfg.color,
        border: `1px solid ${cfg.border}`,
        flexShrink: 0,
        lineHeight: 1.4,
      }}
    >
      {cfg.label}
    </span>
  );
}

// ─── Source card (name + status badge, no description) ───────────────────────
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
        border: `1px solid ${hovered ? C.primary : C.border}`,
        borderRadius: 12,
        padding: "10px 14px",
        boxShadow: C.shadow,
        backgroundColor: C.white,
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
          width: 28,
          height: 28,
          maxWidth: 28,
          objectFit: "contain",
          flexShrink: 0,
        }}
        onError={(e) => {
          e.target.style.display = "none";
        }}
      />

      {/* Name */}
      <span
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: C.titleText,
          flex: 1,
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          minWidth: 0,
        }}
      >
        {source.Title}
      </span>

      {/* Support status badge */}
      {source.support_status && (
        <StatusBadge status={source.support_status} />
      )}
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
  const [searchText, setSearchText] = useState("");

  // Filter by search text only (category filters removed — see TODO at top)
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

  const orderedGroups = CATEGORY_ORDER.filter(
    (t) => grouped[t]?.length > 0
  ).map((t) => [
    t,
    [...grouped[t]].sort((a, b) => a.Title.localeCompare(b.Title)),
  ]);

  // Append any categories not in canonical list (future-proofing)
  const known = new Set(CATEGORY_ORDER);
  Object.keys(grouped)
    .filter((t) => !known.has(t))
    .sort()
    .forEach((t) =>
      orderedGroups.push([
        t,
        [...grouped[t]].sort((a, b) => a.Title.localeCompare(b.Title)),
      ])
    );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 28 }}>
      {/* ── Sticky header: title + description + search ── */}
      <div
        style={{
          position: "sticky",
          top: "var(--ifm-navbar-height, 60px)",
          zIndex: 100,
          backgroundColor: C.white,
          paddingBottom: 16,
          borderBottom: `1px solid ${C.border}`,
          marginBottom: -12,
        }}
      >
        <h1
          style={{
            fontSize: 28,
            fontWeight: 800,
            color: C.titleText,
            margin: "0 0 6px",
          }}
        >
          Sources
        </h1>
        <p
          style={{
            fontSize: 14,
            color: C.descText,
            margin: "0 0 14px",
            lineHeight: 1.5,
          }}
        >
          Explore all the ways you can connect your data tools with DataHub.
        </p>

        {/* Search bar */}
        <div style={{ position: "relative", maxWidth: 480 }}>
          <span
            style={{
              position: "absolute",
              left: 12,
              top: "50%",
              transform: "translateY(-50%)",
              color: C.placeholder,
              pointerEvents: "none",
              display: "flex",
              alignItems: "center",
            }}
          >
            <svg width="15" height="15" viewBox="0 0 20 20" fill="none">
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
              paddingLeft: 34,
              paddingRight: 14,
              borderRadius: 8,
              border: `1px solid ${C.border}`,
              boxShadow: C.shadow,
              fontSize: 13,
              color: C.titleText,
              outline: "none",
              boxSizing: "border-box",
              backgroundColor: C.white,
              transition: "border-color 0.15s, box-shadow 0.15s",
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

      {/* ── Grouped card sections ── */}
      {orderedGroups.length > 0 ? (
        orderedGroups.map(([category, sources]) => (
          <div
            key={category}
            style={{ display: "flex", flexDirection: "column", gap: 10 }}
          >
            <SectionHeader label={category} count={sources.length} />
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
                gap: 6,
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
