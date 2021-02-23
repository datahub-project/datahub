import { execSync } from "child_process";
import * as matter from "gray-matter";
import { readFileSync } from "fs";

// Note: this must be executed within the docs-website directory.

function list_markdown_files(): string[] {
  const all_markdown_files = execSync("cd .. && git ls-files . | grep '.md$'")
    .toString()
    .trim()
    .split("\n");

  const filter_patterns = [
    // We don't need our issue and pull request templates.
    /^\.github/,
    // Ignore everything within this directory.
    /^docs-website/,
    // Don't want hosted docs for these.
    /^datahub-web/,
    /^docs\/rfc\/templates\/000-template.md$/,
  ];

  const markdown_files = all_markdown_files.filter((filepath) => {
    return !filter_patterns.some((rule) => rule.test(filepath));
  });
  return markdown_files;
}

// const markdown_files = list_markdown_files();
// console.log(markdown_files);

function markdown_guess_title(contents: matter.GrayMatterFile<string>): void {
  if (contents.data.title) {
    return;
  }

  // Find first h1 header and use it as the title.
  const header = contents.content.match(/^# (.+)$/m);
  const title = header[1];
  contents.data.title = title;
}

const filepath = "docs/advanced/aspect-versioning.md";

const contents_string = readFileSync(`../${filepath}`).toString();
const contents = matter(contents_string);
markdown_guess_title(contents);
console.log(contents);
