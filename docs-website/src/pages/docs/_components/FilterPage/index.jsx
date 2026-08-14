import React from "react";
import Head from "@docusaurus/Head";
import Layout from "@theme/Layout";
import clsx from "clsx";
import FilterBar from "../FilterBar";
import FilterCards from "../FilterCards";
import styles from "./legend.module.scss";
import { SUPPORT_STATUS_DESCRIPTIONS } from "../supportStatus";

// Decodes the two independent labels shown on each card. Support status
// describes how mature an existing connector is; connection type describes how
// metadata reaches DataHub, and its "API" value means no connector exists at
// all. Readers have mistaken "API" for a support level, so both axes are
// spelled out here rather than left to inference.
function CardLegend() {
  return (
    <details className={styles.legend} open>
      <summary>
        Labels on these cards describe two separate things: how mature a
        connector is, and how metadata reaches DataHub.
      </summary>
      <div className={styles.legendGrid}>
        <div>
          <h4>Support status (badge in the corner of each card)</h4>
          <ul>
            <li>
              <span
                className={clsx(styles.legendSample, styles.sampleCertified)}
              >
                Certified
              </span>
              {SUPPORT_STATUS_DESCRIPTIONS.certified}
            </li>
            <li>
              <span
                className={clsx(styles.legendSample, styles.sampleIncubating)}
              >
                Incubating
              </span>
              {SUPPORT_STATUS_DESCRIPTIONS.incubating}
            </li>
            <li>
              <span className={clsx(styles.legendSample, styles.sampleTesting)}>
                Testing
              </span>
              {SUPPORT_STATUS_DESCRIPTIONS.testing}
            </li>
            <li>
              <b>No badge</b> - the connector does not declare a support status.
            </li>
          </ul>
          <a href="docs/metadata-ingestion/source_overview#metadata-ingestion-source-status">
            More on support status
          </a>
        </div>
        <div>
          <h4>Connection type (how metadata reaches DataHub)</h4>
          <ul>
            <li>
              <b>Pull</b> - DataHub connects to the platform and reads metadata
              on a schedule.
            </li>
            <li>
              <b>Push</b> - the platform sends metadata to DataHub.
            </li>
            <li>
              <span className={clsx(styles.legendSample, styles.sampleApi)}>
                API
              </span>
              <b>No ready-made connector exists.</b> DataHub can model this
              platform, but you emit the metadata yourself using the SDK or
              APIs.
            </li>
          </ul>
          <a href="docs/metadata-ingestion/datahub-skills">
            Build your own integration
          </a>
        </div>
      </div>
    </details>
  );
}

export function FilterPage(
  siteConfig,
  metadata,
  title,
  subtitle,
  {
    allowExclusivity = false,
    useTags = false,
    useFilters = false,
    showLegend = false,
    seoTitle = siteConfig.tagline,
    seoDescription = "DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems.",
  } = {}
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
      // Each item is a documentation page describing a DataHub integration.
      // Using @type "WebPage" (not "SoftwareApplication") because Google's rich
      // result requirements for SoftwareApplication mandate offers and
      // aggregateRating, which don't apply to documentation entries.
      itemListElement: metadata.map((source, i) => ({
        "@type": "ListItem",
        position: i + 1,
        item: {
          "@type": "WebPage",
          name: source.Title,
          description: source.Description,
          about: source.tags?.["Platform Type"] || undefined,
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
    <Layout title={seoTitle} description={seoDescription}>
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

              {showLegend && <CardLegend />}
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
