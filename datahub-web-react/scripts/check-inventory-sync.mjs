#!/usr/bin/env node
/**
 * Fail when eager-budget.json and eager-budget-inventory.json disagree past slack.
 * Keeps the regression table honest after budget-only PRs.
 */
import fs from 'fs';
import { pathToFileURL } from 'url';

import { resolveBudgetPaths, takeBudgetPathArgs } from './budget-paths.mjs';
import { toleranceBytes } from './eager-budget.mjs';

function main(argv = process.argv.slice(2)) {
    const { paths: pathOverrides } = takeBudgetPathArgs(argv);
    const paths = resolveBudgetPaths(pathOverrides);
    const budget = JSON.parse(fs.readFileSync(paths.eagerBudget, 'utf8'));
    const inventory = JSON.parse(fs.readFileSync(paths.inventoryBaseline, 'utf8'));
    const budgetGzip = budget.totalGzip;
    const inventoryGzip = inventory.eager_whole_file_gzip_bytes;
    if (typeof budgetGzip !== 'number' || typeof inventoryGzip !== 'number') {
        console.error('eager-budget.json totalGzip and inventory eager_whole_file_gzip_bytes must be numbers.');
        process.exit(1);
    }
    const slack = toleranceBytes(budgetGzip);
    const delta = Math.abs(budgetGzip - inventoryGzip);
    if (delta > slack) {
        console.error(
            `eager-budget-inventory.json eager_whole_file_gzip_bytes is ${inventoryGzip}, ` +
                `${delta} away from eager-budget.json totalGzip ${budgetGzip} (allowed ±${slack}). ` +
                `Refresh the inventory from the same dist that set the budget:\n` +
                `  node e2e-test/perf/vitals/bundle-inventory.mjs --dist <dist> ` +
                `--write-baseline <eager-budget-inventory.json>`,
        );
        process.exit(1);
    }
    console.log(
        `Inventory in sync with eager budget (budget ${budgetGzip}, inventory ${inventoryGzip}, slack ±${slack}).`,
    );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main();
}
