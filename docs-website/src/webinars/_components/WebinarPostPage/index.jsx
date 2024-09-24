/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import React from "react";
import clsx from "clsx";
import {
  HtmlClassNameProvider,
  ThemeClassNames,
} from "@docusaurus/theme-common";
import { BlogPostProvider } from "@docusaurus/theme-common/internal";
import BlogLayout from "@theme/BlogLayout";
import WebinarPostItem from "../WebinarPostItem";
import BlogPostPageMetadata from "@theme/BlogPostPage/Metadata";

function BlogPostPageContent({ children }) {
  return (
    // Always hide sidebar and TOC for Webinars
    <BlogLayout>
      <WebinarPostItem>{children}</WebinarPostItem>
    </BlogLayout>
  );
}
export default function BlogPostPage(props) {
  const BlogPostContent = props.content;
  return (
    <BlogPostProvider content={props.content} isBlogPostPage>
      <HtmlClassNameProvider
        className={clsx(
          ThemeClassNames.wrapper.blogPages,
          ThemeClassNames.page.blogPostPage
        )}
      >
        <BlogPostPageMetadata />
        <BlogPostPageContent sidebar={props.sidebar}>
          <BlogPostContent />
        </BlogPostPageContent>
      </HtmlClassNameProvider>
    </BlogPostProvider>
  );
}
