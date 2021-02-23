const { execSync } = require("child_process");

// Note: this must be executed within the docs-website directory.

const list_markdown_files = () => {
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
};

const markdown_files = list_markdown_files();
console.log(markdown_files);
