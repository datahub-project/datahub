// Single source of truth for DataHub Cloud release stages.
//
// Imported by:
//   - ./index.js — the FeatureAvailability badge component
//   - ../../../generateDocsDir.ts — to label the served-markdown availability line
//
// Add a new stage here once and both places pick it up automatically.
export const STAGE_META: {
  [key: string]: { label: string; className: string; description: string };
} = {
  alpha: {
    label: "Alpha",
    className: "stageAlpha",
    description:
      "Proof-of-concept, limited to a small set of accounts and built directly with our SE/FDE team. Expect high-touch collaboration and rapid iteration.",
  },
  "private-beta": {
    label: "Private Beta",
    className: "stagePrivateBeta",
    description:
      "Invite-only and evolving rapidly. Contact your DataHub Cloud representative to request access.",
  },
  "public-beta": {
    label: "Public Beta",
    className: "stagePublicBeta",
    description:
      "Available to all DataHub Cloud customers. Mostly stable with minor iteration; we welcome feedback on UX and bugs.",
  },
  ga: {
    label: "Generally Available",
    className: "stageGa",
    description: "Production-ready and fully supported.",
  },
  deprecated: {
    label: "Deprecated",
    className: "stageDeprecated",
    description:
      "This feature is no longer being developed. It will continue to function but will be removed in a future release.",
  },
};
