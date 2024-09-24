import React, { useState } from "react";
import clsx from "clsx";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import {
  PageMetadata,
  HtmlClassNameProvider,
  ThemeClassNames,
} from "@docusaurus/theme-common";
import BlogListPaginator from "@theme/BlogListPaginator";
import SearchMetadata from "@theme/SearchMetadata";
import { BlogPostProvider } from "@docusaurus/theme-common/internal";
import WebinarItemCard from "../WebinarItemCard";
import Layout from "@theme/Layout";
import styles from "./styles.module.scss";

function sortWebinars(items) {
  const now = new Date();

  // Filter out upcoming items 1 hour past their start time
  const filteredItems = items.filter((item) => {
    const frontMatter = item.content.frontMatter;
    if (!frontMatter.recurring && frontMatter.status === "upcoming") {
      const itemDate = new Date(frontMatter.date);
      return itemDate >= new Date(now.getTime() - 60 * 60 * 1000);
    }
    return true;
  });

  // Sort by date ascending
  filteredItems.sort((a, b) => {
    const dateA = new Date(a.content.frontMatter.date);
    const dateB = new Date(b.content.frontMatter.date);
    return dateA - dateB;
  });

  // Always put upcoming first
  filteredItems.sort((a, b) => {
    const statusA = a.content.frontMatter.status;
    const statusB = b.content.frontMatter.status;
    if (statusA < statusB) return 1;
    if (statusA > statusB) return -1;
    return 0;
  });

  // Always put recurring first
  filteredItems.sort((a, b) => {
    const recurringA = a.content.frontMatter.recurring;
    const recurringB = b.content.frontMatter.recurring;
    if (recurringA < recurringB) return 1;
    if (recurringA > recurringB) return -1;
    return 0;
  });

  return filteredItems;
}

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

  const sortedWebinars = sortWebinars(items);

  return (
    <Layout>
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Webinars</h1>
              <p className="hero__subtitle">
                Our live or on-demand webinars can help you gain insights - from
                understanding Acryl Data to discovering how businesses leverage
                it to take back control of their data.{" "}
              </p>
            </div>
          </div>
        </div>
      </header>
      <div className="container">
        <div className="row">
          {(sortedWebinars || []).map(({ content: BlogPostContent }) => (
            <BlogPostProvider
              key={BlogPostContent.metadata.permalink}
              content={BlogPostContent}
            >
              <WebinarItemCard />
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
    <HtmlClassNameProvider
      className={clsx(
        ThemeClassNames.wrapper.blogPages,
        ThemeClassNames.page.blogListPage
      )}
    >
      <BlogListPageMetadata {...props} />
      <BlogListPageContent {...props} />
    </HtmlClassNameProvider>
  );
}
