/**
 * Merges per-spec mochawesome JSONs from all Cypress batches into a single
 * inline HTML report with embedded screenshots.
 *
 * Inputs (written by the workflow before this script runs):
 *   json-file-list.txt       – one JSON path per line
 *   combined-screenshots/    – flat copy of all batch screenshot dirs
 *
 * Output:
 *   cypress-html-report/index.html  – fully self-contained HTML report
 */

const { merge } = require("mochawesome-merge");
const { enhanceReport } = require("cypress-mochawesome-reporter/lib/enhanceReport");
const reportGenerator = require("mochawesome-report-generator");
const fs = require("fs");
const path = require("path");

const jsonFiles = fs
  .readFileSync("json-file-list.txt", "utf-8")
  .trim()
  .split("\n")
  .filter(Boolean);

const screenshotsDir = path.resolve("combined-screenshots");

merge({ files: jsonFiles })
  .then((report) => {
    try {
      enhanceReport(
        report,
        { embeddedScreenshots: true, saveAllAttempts: false },
        screenshotsDir,
      );
    } catch (e) {
      console.warn(
        "Screenshot embedding failed (screenshots may be missing):",
        e.message,
      );
    }
    return reportGenerator.create(report, {
      reportDir: "cypress-html-report",
      reportFilename: "index",
      reportTitle: "Cypress smoke tests",
      inline: true,
    });
  })
  .then((result) => console.log("Report saved:", result))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
