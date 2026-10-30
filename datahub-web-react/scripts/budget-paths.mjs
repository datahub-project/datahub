/**
 * Resolve bundle-budget file locations for OSS and downstream forks.
 *
 * Defaults stay under datahub-web-react/. Forks can point at their own files via
 * env (BUNDLE_BUDGET_DIR, BUNDLE_EAGER_BUDGET, BUNDLE_PAGE_BUDGET,
 * BUNDLE_INVENTORY_BASELINE, BUNDLE_BUDGET_DIST) or CLI flags without conflicting
 * with upstream baselines on sync.
 */
import path from 'path';
import { fileURLToPath } from 'url';

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/**
 * @param {object} [opts]
 * @param {string} [opts.budgetDir]
 * @param {string} [opts.eagerBudget]
 * @param {string} [opts.pageBudget]
 * @param {string} [opts.inventoryBaseline]
 * @param {string} [opts.dist]
 * @param {NodeJS.ProcessEnv} [opts.env]
 */
export function resolveBudgetPaths(opts = {}) {
    const env = opts.env || process.env;
    const budgetDir = path.resolve(opts.budgetDir || env.BUNDLE_BUDGET_DIR || packageRoot);
    return {
        budgetDir,
        eagerBudget: path.resolve(
            opts.eagerBudget || env.BUNDLE_EAGER_BUDGET || path.join(budgetDir, 'eager-budget.json'),
        ),
        pageBudget: path.resolve(
            opts.pageBudget || env.BUNDLE_PAGE_BUDGET || path.join(budgetDir, 'page-budget.json'),
        ),
        inventoryBaseline: path.resolve(
            opts.inventoryBaseline ||
                env.BUNDLE_INVENTORY_BASELINE ||
                path.join(budgetDir, 'eager-budget-inventory.json'),
        ),
        dist: path.resolve(opts.dist || env.BUNDLE_BUDGET_DIST || path.join(budgetDir, 'dist')),
    };
}

/**
 * Parse shared budget-path CLI flags from argv. Mutates nothing; returns
 * `{ rest, paths }` where `paths` are only the keys that were set.
 *
 * @param {string[]} argv
 */
export function takeBudgetPathArgs(argv) {
    const paths = {};
    const rest = [];
    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        const next = () => {
            i += 1;
            return argv[i];
        };
        if (arg === '--budget-dir') paths.budgetDir = next();
        else if (arg === '--eager-budget') paths.eagerBudget = next();
        else if (arg === '--page-budget') paths.pageBudget = next();
        else if (arg === '--inventory-baseline') paths.inventoryBaseline = next();
        else if (arg === '--dist') paths.dist = next();
        else rest.push(arg);
    }
    return { rest, paths };
}
