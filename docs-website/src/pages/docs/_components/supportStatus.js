// Single source for the support-status glossary. The integrations page shows
// these in a legend, and each card repeats the matching one as a hover tooltip;
// keeping one copy stops the two drifting apart.
export const SUPPORT_STATUS_DESCRIPTIONS = {
  certified: "Well-tested and widely adopted; expected to be stable.",
  incubating:
    "Ready for adoption, but not yet tested across a wide variety of edge cases.",
  testing: "Available for experimentation; may change without notice.",
};

export function getSupportStatusDescription(supportStatus) {
  return SUPPORT_STATUS_DESCRIPTIONS[supportStatus.trim().toLowerCase()] || "";
}

// Cards prefix the label, e.g. "Certified: well-tested and widely adopted...".
export function getSupportStatusTooltip(supportStatus) {
  const description = getSupportStatusDescription(supportStatus);
  if (!description) return "";
  const label = supportStatus.trim();
  return `${label}: ${description[0].toLowerCase()}${description.slice(1)}`;
}
