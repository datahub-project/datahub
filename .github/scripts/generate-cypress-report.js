/**
 * Merges per-spec mochawesome JSON files from all Cypress batches into a single
 * HTML report, embedding failure screenshots as base64.
 *
 * Expected environment (set up by the calling workflow step):
 *   - json-file-list.txt  – newline-separated paths to all .jsons/*.json files
 *   - combined-screenshots/ – screenshots from all batches merged into one dir
 *
 * Output: cypress-html-report/index.html (self-contained, inline assets)
 */

const { merge }         = require('mochawesome-merge');
const { enhanceReport } = require('cypress-mochawesome-reporter/lib/enhanceReport');
const reportGenerator   = require('mochawesome-report-generator');
const fs   = require('fs');
const path = require('path');

const jsonFiles      = fs.readFileSync('json-file-list.txt', 'utf-8').trim().split('\n').filter(Boolean);
const screenshotsDir = path.resolve('combined-screenshots');

merge({ files: jsonFiles })
  .then(report => {
    try {
      // enhanceReport converts the cypress-mochawesome-reporter-screenshots context
      // entries (screenshot paths relative to screenshotsFolder) into base64 data URIs.
      enhanceReport(report, { embeddedScreenshots: true, saveAllAttempts: false }, screenshotsDir);
    } catch (e) {
      console.warn('Screenshot embedding failed (screenshots may be missing):', e.message);
    }
    return reportGenerator.create(report, {
      reportDir:      'cypress-html-report',
      reportFilename: 'index',
      reportTitle:    'Cypress smoke tests',
      inline:         true,
    });
  })
  .then(result => console.log('Report saved:', result))
  .catch(err  => { console.error(err); process.exit(1); });
