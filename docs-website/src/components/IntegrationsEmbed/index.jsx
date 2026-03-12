import React from "react";
import FilterBar from "../../pages/docs/_components/FilterBar";
import FilterCards from "../../pages/docs/_components/FilterCards";

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

export default function IntegrationsEmbed({ metadata }) {
  const [textState, setTextState] = React.useState("");
  const [filterState, setFilterState] = React.useState([]);
  const [isExclusive, setIsExclusive] = React.useState(false);

  let filterOptions = {};
  metadata.forEach((data) => {
    const filters = data["tags"];
    Object.keys(filters).forEach((key) => {
      if (filterOptions[key] === undefined) {
        filterOptions[key] = new Set();
      }
      filters[key].split(",").forEach((tag) => {
        if (tag === " " || tag === "") return;
        filterOptions[key].add(tag.trim());
      });
    });
  });

  if (filterOptions["Platform Type"]) {
    filterOptions["Platform Type"] = sortPlatformTypeOptions(
      filterOptions["Platform Type"]
    );
  }

  const filterKeys = Object.keys(filterOptions);

  function getTags(path) {
    if (path === undefined || path === null) return [];
    const data = metadata.find((d) => d.Path === path);
    const recordTags = data?.["tags"];
    if (!recordTags) return [];
    const tags = [];
    filterKeys.forEach((key) => {
      if (!recordTags[key]) return;
      recordTags[key].split(",").forEach((feature) => {
        if (feature === " " || feature === "") return;
        tags.push(feature.trim());
      });
    });
    return tags;
  }

  const content = metadata.map((source) => ({
    title: source.Title,
    image: source.imgPath,
    description: source.Description,
    tags: getTags(source.Path),
    filters: source.tags,
    to: source.Path,
    useFilters: false,
    useTags: true,
    filterState,
  }));

  const filtered = content.filter((item) => {
    if (textState === "" && filterState.length === 0) return true;
    if (filterState.length > 0) {
      let flag = isExclusive;
      filterState.forEach((filter) => {
        flag =
          (!isExclusive && (flag || item.tags.includes(filter))) ||
          (isExclusive && flag && item.tags.includes(filter));
      });
      return flag;
    }
    return (
      item.title.toLowerCase().includes(textState.toLowerCase()) ||
      item.description.toLowerCase().includes(textState.toLowerCase())
    );
  });

  return (
    <div>
      <FilterBar
        textState={textState}
        setTextState={setTextState}
        filterState={filterState}
        setFilterState={setFilterState}
        filterOptions={filterOptions}
        allowExclusivity={false}
        setIsExclusive={setIsExclusive}
      />
      <FilterCards content={filtered} filterBar={<FilterBar />} />
    </div>
  );
}
