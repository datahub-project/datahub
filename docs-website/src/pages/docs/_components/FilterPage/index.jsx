import React from "react";
import Layout from "@theme/Layout";
import FilterBar from "../FilterBar";
import FilterCards from "../FilterCards";

export function FilterPage(siteConfig, metadata, title, subtitle) {
  const [textState, setTextState] = React.useState("");
  const [filterState, setFilterState] = React.useState([]);

  const filterOptions = {
    Difficulty: new Set(),
    "Platform Type": new Set(),
    "Connection Type": new Set(),
    Features: new Set(),
  };
  function getTags(path) {
    if (path === undefined || path === null) return;
    const data = metadata.find((data) => {
      if (data.Path === path) {
        return data;
      }
    });
    let tags = [];
    if (data === undefined) return tags;
    Object.keys(data).map((key) => {
      if (key === "Features") {
        data[key].split(",").forEach((feature) => {
          if (feature === " " || feature === "") return;
          tags.push(feature.trim());
        });
      } else if (
        key !== "Path" &&
        key !== "Title" &&
        key !== "Description" &&
        key !== "logoPath"
      )
        tags.push(data[key]);
    });
    return tags;
  }

  metadata.forEach((data) => {
    Object.keys(data).map((key) => {
      if (key === "Features") {
        data[key].split(",").forEach((feature) => {
          if (feature === " " || feature === "") return;
          filterOptions[key].add(feature.trim());
        });
      } else if (
        key !== "Path" &&
        key !== "Title" &&
        key !== "Description" &&
        key !== "logoPath"
      )
        filterOptions[key].add(data[key]);
    });
  });

  const ingestionSourceContent = metadata.map((source) => {
    return {
      title: source.Title,
      platformIcon: source.logoPath,
      description: source.Description,
      tags: getTags(source.Path),
      to: source.Path,
    };
  });
  const filteredIngestionSourceContent = ingestionSourceContent.filter(
    (item) => {
      if (textState === "" && filterState.length === 0) return true;
      else if (filterState.length > 0) {
        let flag = false;
        filterState.forEach((filter) => {
          if (item.tags.includes(filter)) flag = true;
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
              />
            </div>
          </div>
        </div>
      </header>

      <FilterCards
        content={filteredIngestionSourceContent}
        filterBar={<FilterBar />}
      />
    </Layout>
  );
}

export default FilterPage;
