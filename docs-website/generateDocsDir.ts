import { execSync } from "child_process";
import * as matter from "gray-matter";
import * as fs from "fs";
import * as path from "path";

// Note: this must be executed within the docs-website directory.

// Constants.
const GITHUB_BROWSE_URL = "https://github.com/linkedin/datahub/blob/master";

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
    /^datahub-web\//,
    /^docs\/rfc\/templates\/000-template\.md$/,
    /^docs\/docker\/README\.md/, // This one is just a pointer to another file.
  ];

  const markdown_files = all_markdown_files.filter((filepath) => {
    return !filter_patterns.some((rule) => rule.test(filepath));
  });
  return markdown_files;
}

const markdown_files = list_markdown_files();
// console.log(markdown_files);

function markdown_guess_title(contents: matter.GrayMatterFile<string>): void {
  if (contents.data.title) {
    return;
  }

  // Find first h1 header and use it as the title.
  const header = contents.content.match(/^# (.+)$/m);
  const title = header[1];
  contents.data.title = title;
  contents.data.hide_title = true;
}

function new_url(original: string, filepath: string): string {
  if (original.startsWith("http://") || original.startsWith("https://")) {
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
    ].some((ext) => suffix.startsWith(ext))
  ) {
    // A reference to a file or directory in the Github repo.
    const relation = path.dirname(filepath);
    const updated = path.normalize(
      `${GITHUB_BROWSE_URL}/${relation}/${original}`
    );
    return updated;
  } else if (suffix.startsWith(".md")) {
    // Leave as-is.
    // We use startsWith here so that we can allow anchor tags on links.
    return original;
  } else if ([".png", ".svg", ".pdf"].includes(suffix)) {
    // Let docusaurus bundle these as static assets.
    const up_levels = (filepath.match(/\//g) ?? []).length;
    const relation = path.dirname(filepath);
    const updated = path.normalize(
      `${"../".repeat(up_levels + 2)}/${relation}/${original}`
    );
    return updated;
  } else {
    throw `unknown extension - ${original} in ${filepath}`;
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

for (const filepath of markdown_files) {
  console.log("Processing:", filepath);
  const contents_string = fs.readFileSync(`../${filepath}`).toString();
  const contents = matter(contents_string);
  markdown_guess_title(contents);
  markdown_rewrite_urls(contents, filepath);
  // console.log(contents);

  const outpath = `genDocs/${filepath}`;
  const pathname = path.dirname(outpath);
  fs.mkdirSync(pathname, { recursive: true });
  fs.writeFileSync(outpath, contents.stringify(""));
}
