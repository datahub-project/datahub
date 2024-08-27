import React from "react";
import Layout from "@theme/Layout";
import FilterBar from "../FilterBar";
import FilterCards from "../FilterCards";

export function FilterPage(
  siteConfig,
  metadata,
  title,
  subtitle,
  allowExclusivity = false,
  useTags = false,
  useFilters = false
) {
  const [textState, setTextState] = React.useState("");
  const [filterState, setFilterState] = React.useState([]);
  const [isExclusive, setIsExclusive] = React.useState(false);

  let filterOptions = {};
  metadata.forEach((data) => {
    const filters = data["tags"];
    Object.keys(filters).map((key) => {
      if (filterOptions[key] === undefined) {
        filterOptions[key] = new Set();
      }
      filters[key].split(",").forEach((tag) => {
        if (tag === " " || tag === "") return;
        filterOptions[key].add(tag.trim());
      });
    });
  });
  const filterKeys = Object.keys(filterOptions);
  function getTags(path) {
    if (path === undefined || path === null) return;
    const data = metadata.find((data) => {
      if (data.Path === path) {
        return data;
      }
    });
    const recordTags = data["tags"];
    let tags = [];
    if (recordTags === undefined) return tags;
    filterKeys.map((key) => {
      if (recordTags[key] === undefined || recordTags[key] === null) return;
      recordTags[key].split(",").forEach((feature) => {
        if (feature === " " || feature === "") return;
        tags.push(feature.trim());
      });
    });
    return tags;
  }

  const ingestionSourceContent = metadata.map((source) => {
    return {
      title: source.Title,
      image: source.imgPath,
      description: source.Description,
      tags: getTags(source.Path),
      filters: source.tags,
      to: source.Path,
      useFilters: useFilters,
      useTags: useTags,
      filterState: filterState,
    };
  });
  const filteredIngestionSourceContent = ingestionSourceContent.filter(
    (item) => {
      if (textState === "" && filterState.length === 0) return true;
      else if (filterState.length > 0) {
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
    }
  );

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">{title}</h1>
              <p className="hero__subtitle">{subtitle}</p>

              <FilterBar
                textState={textState}
                setTextState={setTextState}
                filterState={filterState}
                setFilterState={setFilterState}
                filterOptions={filterOptions}
                allowExclusivity={allowExclusivity}
                setIsExclusive={setIsExclusive}
              />
            </div>
          </div>
        </div>
      </header>

      <FilterCards
        content={filteredIngestionSourceContent}
        filterBar={<FilterBar />}
      />
      <br />
      <br />
    </Layout>
  );
}

export default FilterPage;
