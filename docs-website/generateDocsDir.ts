import { execSync } from "child_process";
import * as matter from "gray-matter";
import * as fs from "fs";
import * as path from "path";

// Note: this must be executed within the docs-website directory.

// Constants.
const HOSTED_SITE_URL = "https://datahubproject.io";
const GITHUB_EDIT_URL = "https://github.com/linkedin/datahub/blob/master";
const GITHUB_BROWSE_URL = "https://github.com/linkedin/datahub/blob/master";

const SIDEBARS_DEF_PATH = "./sidebars.js";
const sidebars = require(SIDEBARS_DEF_PATH);
const sidebars_json = JSON.stringify(sidebars);
const sidebars_text = fs.readFileSync(SIDEBARS_DEF_PATH).toString();
console.log(sidebars_json);

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
  const all_markdown_files = execSync("cd .. && git ls-files . | grep '.md$'")
    .toString()
    .trim()
    .split("\n");

  const filter_patterns = [
    // We don't need our issue and pull request templates.
    /^\.github\//,
    // Ignore everything within this directory.
    /^docs-website\//,
    // Don't want hosted docs for these.
    /^contrib\//,
    // Keep main docs for kubernetes, but skip the inner docs
    /^datahub-kubernetes\/datahub\//,
    /^datahub-web\//,
    /^metadata-ingestion-examples\//,
    /^docs\/rfc\/templates\/000-template\.md$/,
    /^docs\/docker\/README\.md/, // This one is just a pointer to another file.
    /^docs\/README\.md/, // This one is just a pointer to the hosted docs site.
  ];

  const markdown_files = all_markdown_files.filter((filepath) => {
    return !filter_patterns.some((rule) => rule.test(filepath));
  });
  return markdown_files;
}

const markdown_files = list_markdown_files();
// console.log(markdown_files);

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
  if (contents.data.title) {
    return;
  }

  let title: string;
  if (filepath in hardcoded_titles) {
    title = hardcoded_titles[filepath];
    if (filepath in hardcoded_descriptions) {
      contents.data.description = hardcoded_descriptions[filepath];
    }
  } else {
    // Find first h1 header and use it as the title.
    const headers = contents.content.match(/^# (.+)$/gm);
    if (headers.length > 1 && contents.content.indexOf("```") < 0) {
      throw new Error(`too many h1 headers in ${filepath}`);
    }
    title = headers[0].slice(2).trim();
    if (title.startsWith("DataHub ")) {
      title = title.slice(8).trim();
    }
  }

  contents.data.title = title;
  contents.data.hide_title = true;
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
        .startsWith("https://github.com/linkedin/datahub/blob") ||
        original
          .toLowerCase()
          .startsWith("https://github.com/linkedin/datahub/tree")) &&
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
      /\[(.+?)\]\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)/g,
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

for (const filepath of markdown_files) {
  // console.log("Processing:", filepath);
  const contents_string = fs.readFileSync(`../${filepath}`).toString();
  const contents = matter(contents_string);

  markdown_guess_title(contents, filepath);
  markdown_add_slug(contents, filepath);
  markdown_add_edit_url(contents, filepath);
  markdown_rewrite_urls(contents, filepath);
  markdown_enable_specials(contents, filepath);
  // console.log(contents);

  const outpath = `genDocs/${filepath}`;
  const pathname = path.dirname(outpath);
  fs.mkdirSync(pathname, { recursive: true });
  fs.writeFileSync(outpath, contents.stringify(""));
}

// Error if a doc is not accounted for in a sidebar.
const autogenerated_sidebar_directories = ["docs/rfc/active/"];
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
      `File not accounted for in sidebar ${filepath} - try adding it to docs-website/sidebars.js`
    );
  }
}
