import { execSync } from "child_process";
import * as matter from "gray-matter";
import * as fs from "fs";
import * as path from "path";
import { Octokit } from "@octokit/rest";
import { throttling } from "@octokit/plugin-throttling";
import { retry } from "@octokit/plugin-retry";

// Note: this must be executed within the docs-website directory.

// Constants.
const HOSTED_SITE_URL = "https://datahubproject.io";
const GITHUB_EDIT_URL =
  "https://github.com/datahub-project/datahub/blob/master";
const GITHUB_BROWSE_URL =
  "https://github.com/datahub-project/datahub/blob/master";

const OUTPUT_DIRECTORY = "genDocs";

const SIDEBARS_DEF_PATH = "./sidebars.js";
const sidebars = require(SIDEBARS_DEF_PATH);
const sidebars_json = JSON.stringify(sidebars);
const sidebars_text = fs.readFileSync(SIDEBARS_DEF_PATH).toString();

const MyOctokit = Octokit.plugin(retry).plugin(throttling);
const octokit = new MyOctokit({
  throttle: {
    onRateLimit: (retryAfter, options) => {
      // Retry twice after rate limit is hit.
      if (options.request.retryCount <= 2) {
        return true;
      }
    },
    onAbuseLimit: () => {
      console.warn("GitHub API hit abuse limit");
    },
  },
});

function actually_in_sidebar(filepath: string): boolean {
  const doc_id = get_id(filepath);
  return sidebars_json.indexOf(`"${doc_id}"`) >= 0;
}

function accounted_for_in_sidebar(filepath: string): boolean {
  if (actually_in_sidebar(filepath)) {
    return true;
  }

  // If we want to explicitly not include a doc in the sidebar but still have it
  // available via a direct link, then keeping it commented out in the sidebar
  // serves this purpose. To make it work, we search the text of the sidebar JS
  // file.
  const doc_id = get_id(filepath);
  if (sidebars_text.indexOf(`"${doc_id}"`) >= 0) {
    return true;
  }

  return false;
}

function list_markdown_files(): string[] {
  let all_markdown_files = execSync("git ls-files --full-name .. | grep '.md$'")
    .toString()
    .trim()
    .split("\n");
  let all_generated_markdown_files = execSync(
    "cd .. && ls docs/generated/**/**/*.md"
  )
    .toString()
    .trim()
    .split("\n");

  all_markdown_files = [...all_markdown_files, ...all_generated_markdown_files];

  if (!process.env.CI) {
    // If not in CI, we also include "untracked" files.
    const untracked_files = execSync(
      "(git ls-files --full-name --others --exclude-standard .. | grep '.md$') || true"
    )
      .toString()
      .trim()
      .split("\n")
      .filter((filepath) => !all_generated_markdown_files.includes(filepath));

    if (untracked_files.length > 0) {
      console.log(`Including untracked files in docs list: ${untracked_files}`);
      all_markdown_files = [...all_markdown_files, ...untracked_files];
    }
  }

  const filter_patterns = [
    // We don't need our issue and pull request templates.
    /^\.github\//,
    // Ignore everything within this directory.
    /^docs-website\//,
    // Don't want hosted docs for these.
    /^contrib\//,
    // Keep main docs for kubernetes, but skip the inner docs.
    /^datahub-kubernetes\//,
    // Various other docs/directories to ignore.
    /^metadata-models\/docs\//, // these are used to generate docs, so we don't want to consider them here
    /^metadata-ingestion\/archived\//, // these are archived, so we don't want to consider them here
    /^metadata-ingestion\/docs\/sources\//, // these are used to generate docs, so we don't want to consider them here
    /^metadata-ingestion-examples\//,
    /^docker\/(?!README|datahub-upgrade|airflow\/local_airflow)/, // Drop all but a few docker docs.
    /^docs\/rfc\/templates\/000-template\.md$/,
    /^docs\/docker\/README\.md/, // This one is just a pointer to another file.
    /^docs\/README\.md/, // This one is just a pointer to the hosted docs site.
    /^\s*$/, //Empty string
  ];

  const markdown_files = all_markdown_files.filter((filepath) => {
    return !filter_patterns.some((rule) => rule.test(filepath));
  });
  return markdown_files;
}

const markdown_files = list_markdown_files();
// for (let markdown_file of markdown_files) {
//   console.log("markdown_file", markdown_file);
// }

function get_id(filepath: string): string {
  // Removes the file extension (e.g. md).
  const id = filepath.replace(/\.[^/.]+$/, "");
  // console.log(id);
  return id;
}

const hardcoded_slugs = {
  "README.md": "/",
};

function get_slug(filepath: string): string {
  // The slug is the URL path to the page.
  // In the actual site, all slugs are prefixed with /docs.
  // There's no need to do this cleanup, but it does make the URLs a bit more aesthetic.

  if (filepath in hardcoded_slugs) {
    return hardcoded_slugs[filepath];
  }

  let slug = get_id(filepath);
  if (slug.startsWith("docs/")) {
    slug = slug.slice(5);
  }
  slug = `/${slug}`;
  if (slug.endsWith("/README")) {
    slug = slug.slice(0, -7);
  }
  slug = slug.toLowerCase();
  return slug;
}

const hardcoded_titles = {
  "README.md": "Introduction",
  "docs/demo.md": "Demo",
};
// titles that have been hardcoded in sidebars.js
// (for cases where doc is reference multiple times with different titles)
const sidebarsjs_hardcoded_titles = [
  "metadata-ingestion/README.md",
  "metadata-ingestion/source_docs/s3.md",
  "docs/api/graphql/overview.md",
];
const hardcoded_hide_title = ["README.md"];

const hardcoded_descriptions = {
  // Only applied if title is also overridden.
  "README.md":
    "DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems.",
};

// FIXME: Eventually, we'd like to fix all of the broken links within these files.
const allowed_broken_links = [
  "docs/developers.md",
  "docs/how/customize-elasticsearch-query-template.md",
  "docs/how/graph-onboarding.md",
  "docs/how/search-onboarding.md",
  "docs/how/build-metadata-service.md",
];

function markdown_guess_title(
  contents: matter.GrayMatterFile<string>,
  filepath: string
): void {
  if (sidebarsjs_hardcoded_titles.includes(filepath)) {
    return;
  }

  if (contents.data.title) {
    contents.data.sidebar_label = contents.data.title;
    return;
  }

  let title: string;
  if (filepath in hardcoded_titles) {
    title = hardcoded_titles[filepath];
    if (filepath in hardcoded_descriptions) {
      contents.data.description = hardcoded_descriptions[filepath];
    }
    if (hardcoded_hide_title.includes(filepath)) {
      contents.data.hide_title = true;
    }
  } else {
    // Find first h1 header and use it as the title.
    const headers = contents.content.match(/^# (.+)$/gm);

    if (!headers) {
      throw new Error(
        `${filepath} must have at least one h1 header for setting the title`
      );
    }

    if (headers.length > 1 && contents.content.indexOf("```") < 0) {
      throw new Error(`too many h1 headers in ${filepath}`);
    }
    title = headers[0].slice(2).trim();
  }

  contents.data.title = title;

  let sidebar_label = title;
  if (sidebar_label.startsWith("DataHub ")) {
    sidebar_label = sidebar_label.slice(8).trim();
  }
  contents.data.sidebar_label = sidebar_label;
}

function markdown_add_edit_url(
  contents: matter.GrayMatterFile<string>,
  filepath: string
): void {
  const editUrl = `${GITHUB_EDIT_URL}/${filepath}`;
  contents.data.custom_edit_url = editUrl;
}

function markdown_add_slug(
  contents: matter.GrayMatterFile<string>,
  filepath: string
): void {
  if (contents.data.slug) {
    return;
  }

  const slug = get_slug(filepath);
  contents.data.slug = slug;
}

function new_url(original: string, filepath: string): string {
  if (original.toLowerCase().startsWith(HOSTED_SITE_URL)) {
    // For absolute links to the hosted docs site, we transform them into local ones.
    // Note that HOSTED_SITE_URL does not have a trailing slash, so after the replacement,
    // the url will start with a slash.
    return original.replace(HOSTED_SITE_URL, "");
  }

  if (original.startsWith("http://") || original.startsWith("https://")) {
    if (
      (original
        .toLowerCase()
        .startsWith("https://github.com/datahub-project/datahub/blob") ||
        original
          .toLowerCase()
          .startsWith("https://github.com/datahub-project/datahub/tree")) &&
      (original.endsWith(".md") || original.endsWith(".pdf"))
    ) {
      throw new Error(`absolute link (${original}) found in ${filepath}`);
    }
    return original;
  }

  if (original.startsWith("#")) {
    // These are anchor links that reference within the document itself.
    return original;
  }

  // Now we assume this is a local reference.
  const suffix = path.extname(original);
  if (
    suffix == "" ||
    [
      ".java",
      ".conf",
      ".xml",
      ".pdl",
      ".json",
      ".py",
      ".ts",
      ".yml",
      ".sh",
      ".env",
      ".sql",
      // Using startsWith since some URLs will be .ext#LINENO
    ].some((ext) => suffix.startsWith(ext))
  ) {
    // A reference to a file or directory in the Github repo.
    const relation = path.dirname(filepath);
    const updated_path = path.normalize(`${relation}/${original}`);
    const check_path = updated_path.replace(/#.+$/, "");
    if (
      !fs.existsSync(`../${check_path}`) &&
      actually_in_sidebar(filepath) &&
      !allowed_broken_links.includes(filepath)
    ) {
      // Detects when the path is a dangling reference, according to the locally
      // checked out repo.
      throw new Error(
        `broken github repo link to ${updated_path} in ${filepath}`
      );
    }
    const updated_url = `${GITHUB_BROWSE_URL}/${updated_path}`;
    return updated_url;
  } else if (suffix.startsWith(".md")) {
    // Leave as-is.
    // We use startsWith above so that we can allow anchor tags on links.
    return original;
  } else if ([".png", ".svg", ".gif", ".pdf"].includes(suffix)) {
    // Let docusaurus bundle these as static assets.
    const up_levels = (filepath.match(/\//g) ?? []).length;
    const relation = path.dirname(filepath);
    const updated = path.normalize(
      `${"../".repeat(up_levels + 2)}/${relation}/${original}`
    );
    //console.log(`Rewriting ${original} ${filepath} as ${updated}`);
    return updated;
  } else {
    throw new Error(`unknown extension - ${original} in ${filepath}`);
  }
}

function markdown_rewrite_urls(
  contents: matter.GrayMatterFile<string>,
  filepath: string
): void {
  const new_content = contents.content
    .replace(
      // Look for the [text](url) syntax. Note that this will also capture images.
      //
      // We do a little bit of parenthesis matching here to account for parens in URLs.
      // See https://stackoverflow.com/a/17759264 for explanation of the second capture group.
      /\[(.*?)\]\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)/g,
      (_, text, url) => {
        const updated = new_url(url.trim(), filepath);
        return `[${text}](${updated})`;
      }
    )
    .replace(
      // Also look for the [text]: url syntax.
      /^\[(.+?)\]\s*:\s*(.+?)\s*$/gm,
      (_, text, url) => {
        const updated = new_url(url, filepath);
        return `[${text}]: ${updated}`;
      }
    );
  contents.content = new_content;
}

function markdown_enable_specials(
  contents: matter.GrayMatterFile<string>,
  filepath: string
): void {
  const new_content = contents.content
    .replace(/^<!--HOSTED_DOCS_ONLY$/gm, "")
    .replace(/^HOSTED_DOCS_ONLY-->$/gm, "");
  contents.content = new_content;
}

function markdown_sanitize_and_linkify(content: string): string {
  // MDX escaping
  content = content.replace(/</g, "&lt;");

  // Link to user profiles.
  content = content.replace(/@([\w-]+)\b/g, "[@$1](https://github.com/$1)");

  // Link to issues/pull requests.
  content = content.replace(
    /#(\d+)\b/g,
    "[#$1](https://github.com/datahub-project/datahub/pull/$1)"
  );

  // Prettify bare links to PRs.
  content = content.replace(
    /(\s+)(https:\/\/github\.com\/linkedin\/datahub\/pull\/(\d+))(\s+|$)/g,
    "$1[#$3]($2)$4"
  );

  return content;
}

function pretty_format_date(datetime: string): string {
  const d = new Date(Date.parse(datetime));
  return d.toDateString();
}

function make_link_anchor(text: string): string {
  return text.replace(/\./g, "-");
}

async function generate_releases_markdown(): Promise<
  matter.GrayMatterFile<string>
> {
  const contents = matter(`---
title: DataHub Releases
sidebar_label: Releases
slug: /releases
custom_edit_url: https://github.com/datahub-project/datahub/blob/master/docs-website/generateDocsDir.ts
---

# DataHub Releases

## Summary\n\n`);

  const releases_list = await octokit.rest.repos.listReleases({
    owner: "linkedin",
    repo: "datahub",
  });

  // Construct a summary table.
  let pastVersionCutoff = false;
  const releaseNoteVersions = new Set();
  contents.content += "| Version | Release Date | Links |\n";
  contents.content += "| ------- | ------------ | ----- |\n";
  for (const release of releases_list.data) {
    let row = `| **${release.tag_name}** | ${pretty_format_date(
      release.created_at
    )} |`;
    if (release.tag_name == "v0.6.1") {
      pastVersionCutoff = true;
    } else if (!pastVersionCutoff) {
      row += `[Release Notes](#${make_link_anchor(release.tag_name)}), `;
      releaseNoteVersions.add(release.tag_name);
    }
    row += `[View on GitHub](${release.html_url}) |\n`;
    contents.content += row;
  }
  contents.content += "\n\n";

  // Full details
  for (const release of releases_list.data) {
    let body: string;
    if (releaseNoteVersions.has(release.tag_name)) {
      body = release.body;
      body = markdown_sanitize_and_linkify(body);

      // Redo the heading levels. First we find the min heading level, and then
      // adjust the markdown headings so that the min heading is level 3.
      const heading_regex = /^(#+)\s/gm;
      const max_heading_level = Math.min(
        3,
        ...[...body.matchAll(heading_regex)].map((v) => v[1].length)
      );
      body = body.replace(
        heading_regex,
        `${"#".repeat(3 - max_heading_level)}$1 `
      );
    } else {
      // Link to GitHub.
      body = `View the [release notes](${release.html_url}) for ${release.name} on GitHub.`;
    }

    const info = `## [${release.name}](${
      release.html_url
    }) {#${make_link_anchor(release.tag_name)}}

Released on ${pretty_format_date(release.created_at)} by [@${
      release.author.login
    }](${release.author.html_url}).

${body}\n\n`;

    contents.content += info;
  }

  return contents;
}

function write_markdown_file(
  contents: matter.GrayMatterFile<string>,
  output_filepath: string
): void {
  const pathname = path.dirname(output_filepath);
  fs.mkdirSync(pathname, { recursive: true });
  try {
    fs.writeFileSync(output_filepath, contents.stringify(""));
  } catch (error) {
    console.log(`Failed to write file ${output_filepath}`);
    console.log(`contents = ${contents}`);
    throw error;
  }
}

(async function main() {
  for (const filepath of markdown_files) {
    //console.log("Processing:", filepath);
    const contents_string = fs.readFileSync(`../${filepath}`).toString();
    const contents = matter(contents_string);

    markdown_guess_title(contents, filepath);
    markdown_add_slug(contents, filepath);
    markdown_add_edit_url(contents, filepath);
    markdown_rewrite_urls(contents, filepath);
    markdown_enable_specials(contents, filepath);
    // console.log(contents);

    const out_path = `${OUTPUT_DIRECTORY}/${filepath}`;
    write_markdown_file(contents, out_path);
  }

  // Generate the releases history.
  {
    const contents = await generate_releases_markdown();
    write_markdown_file(contents, `${OUTPUT_DIRECTORY}/releases.md`);
    markdown_files.push("releases.md");
  }

  // Error if a doc is not accounted for in a sidebar.
  const autogenerated_sidebar_directories = [
    "docs/generated/metamodel",
    "docs/generated/ingestion",
    "metadata-ingestion/archived",
  ];
  for (const filepath of markdown_files) {
    if (
      autogenerated_sidebar_directories.some((dir) => filepath.startsWith(dir))
    ) {
      // The sidebars for these directories is automatically generated,
      // so we don't need check that they're in the sidebar.
      continue;
    }
    if (!accounted_for_in_sidebar(filepath)) {
      throw new Error(
        `File not accounted for in sidebar: ${filepath} - try adding it to docs-website/sidebars.js`
      );
    }
  }
})();
