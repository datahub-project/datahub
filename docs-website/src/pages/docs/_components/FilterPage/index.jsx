import React from "react";
import Head from "@docusaurus/Head";
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
  const categoryCounts = {};
  metadata.forEach((data) => {
    const filters = data["tags"];
    Object.keys(filters).map((key) => {
      if (filterOptions[key] === undefined) {
        filterOptions[key] = new Set();
      }
      filters[key].split(",").forEach((tag) => {
        if (tag === " " || tag === "") return;
        const trimmed = tag.trim();
        filterOptions[key].add(trimmed);
        if (key === "Platform Type") {
          categoryCounts[trimmed] = (categoryCounts[trimmed] || 0) + 1;
        }
      });
    });
  });
  const filterKeys = Object.keys(filterOptions);
  function getTagsFromRecord(recordTags) {
    if (!recordTags) return [];
    let tags = [];
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
      tags: getTagsFromRecord(source.tags),
      filters: source.tags,
      to: source.Path,
      useFilters: useFilters,
      useTags: useTags,
      filterState: filterState,
      isApiConnector: source.isApiConnector || false,
      requestNativeUrl: source.requestNativeUrl || null,
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

  const collectionPageJsonLd = {
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name: title,
    description: subtitle,
    url: "https://docs.datahub.com/integrations",
    mainEntity: {
      "@type": "ItemList",
      numberOfItems: metadata.length,
      itemListElement: metadata.map((source, i) => ({
        "@type": "ListItem",
        position: i + 1,
        item: {
          "@type": "SoftwareApplication",
          name: source.Title,
          description: source.Description,
          applicationCategory: source.tags?.["Platform Type"] || undefined,
          url: source.Path
            ? `https://docs.datahub.com/${source.Path}`
            : undefined,
        },
      })),
    },
  };

  const breadcrumbJsonLd = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      {
        "@type": "ListItem",
        position: 1,
        name: "Docs",
        item: "https://docs.datahub.com/docs",
      },
      {
        "@type": "ListItem",
        position: 2,
        name: "Integrations",
        item: "https://docs.datahub.com/integrations",
      },
    ],
  };

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <Head>
        <script type="application/ld+json">
          {JSON.stringify(collectionPageJsonLd)}
        </script>
        <script type="application/ld+json">
          {JSON.stringify(breadcrumbJsonLd)}
        </script>
      </Head>
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
                categoryCounts={categoryCounts}
              />
            </div>
          </div>
        </div>
      </header>

      <FilterCards
        content={filteredIngestionSourceContent}
        filterBar={<FilterBar />}
      />

      <div
        style={{
          textAlign: "center",
          padding: "2rem 1rem",
          fontSize: "1rem",
          color: "var(--ifm-color-emphasis-700)",
        }}
      >
        Don&apos;t see your data source?{" "}
        <a href="docs/metadata-ingestion/request-connector">
          Request a Connector
        </a>
        {" | "}
        <a href="docs/metadata-ingestion/datahub-skills">Build Your Own</a>
      </div>
    </Layout>
  );
}

export default FilterPage;
