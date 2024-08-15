import React, { useState } from "react";
import clsx from "clsx";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { PageMetadata, HtmlClassNameProvider, ThemeClassNames } from "@docusaurus/theme-common";
import BlogListPaginator from "@theme/BlogListPaginator";
import SearchMetadata from "@theme/SearchMetadata";
import { BlogPostProvider } from "@docusaurus/theme-common/internal";
import LearnItemCard from "../LearnItemCard";
import Layout from "@theme/Layout";
import styles from "./styles.module.scss";

function BlogListPageMetadata(props) {
  const { metadata } = props;
  const {
    siteConfig: { title: siteTitle },
  } = useDocusaurusContext();
  const { blogDescription, blogTitle, permalink } = metadata;
  const isBlogOnlyMode = permalink === "/";
  const title = isBlogOnlyMode ? siteTitle : blogTitle;
  return (
    <>
      <PageMetadata title={title} description={blogDescription} />
      <SearchMetadata tag="blog_posts_list" />
    </>
  );
}

function BlogListPageContent(props) {
  const { metadata, items } = props;
  const [activeFilters, setActiveFilters] = useState([]);
  // These are currently hardcoded, check the frontmatter of the blog posts to see what audiences are available
  const audiences = ["Data Governance Leads", "Data Engineers", "Data Architects", "Data Platform Leads", "Data Analysts"];

  const filteredItems = activeFilters?.length
    ? (items || []).filter((post) => activeFilters.some((activeFilter) => post?.content?.frontMatter?.audience?.some((a) => a === activeFilter)))
    : items;

  const handleFilterToggle = (audience) => {
    if (activeFilters.includes(audience)) {
      setActiveFilters(activeFilters.filter((filter) => filter !== audience));
    } else {
      setActiveFilters([...new Set([...activeFilters, audience])]);
    }
  };

  return (
    <Layout>
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">DataHub Learn</h1>
              <p className="hero__subtitle">Learn about the hot topics in the data ecosystem and how DataHub can help you with your data journey.</p>
            </div>
          </div>
          <div className={styles.filterBar}>
            <strong>For: </strong>
            {audiences.map((audience) => (
              <button
                className={clsx(styles.button, "button button--secondary", { [styles.buttonActive]: activeFilters.includes(audience) })}
                onClick={() => handleFilterToggle(audience)}
                key={audience}
              >
                {audience}
              </button>
            ))}
          </div>
        </div>
      </header>
      <div className="container">
        <div className="row">
          {(filteredItems || []).map(({ content: BlogPostContent }) => (
            <BlogPostProvider key={BlogPostContent.metadata.permalink} content={BlogPostContent}>
              <LearnItemCard />
            </BlogPostProvider>
          ))}
        </div>
        <BlogListPaginator metadata={metadata} />
      </div>
    </Layout>
  );
}

export default function BlogListPage(props) {
  return (
    <HtmlClassNameProvider className={clsx(ThemeClassNames.wrapper.blogPages, ThemeClassNames.page.blogListPage)}>
      <BlogListPageMetadata {...props} />
      <BlogListPageContent {...props} />
    </HtmlClassNameProvider>
  );
}