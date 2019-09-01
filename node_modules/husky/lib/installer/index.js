"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const pkg_dir_1 = __importDefault(require("pkg-dir"));
const getConf_1 = __importDefault(require("../getConf"));
const getScript_1 = __importDefault(require("./getScript"));
const is_1 = require("./is");
const hookList = [
    'applypatch-msg',
    'pre-applypatch',
    'post-applypatch',
    'pre-commit',
    'prepare-commit-msg',
    'commit-msg',
    'post-commit',
    'pre-rebase',
    'post-checkout',
    'post-merge',
    'pre-push',
    'pre-receive',
    'update',
    'post-receive',
    'post-update',
    'push-to-checkout',
    'pre-auto-gc',
    'post-rewrite',
    'sendemail-validate'
];
function writeHook(filename, script) {
    fs_1.default.writeFileSync(filename, script, 'utf-8');
    fs_1.default.chmodSync(filename, 0o0755);
}
function createHook(filename, script) {
    // Get name, used for logging
    const name = path_1.default.basename(filename);
    // Check if hook exist
    if (fs_1.default.existsSync(filename)) {
        const hook = fs_1.default.readFileSync(filename, 'utf-8');
        // Migrate
        if (is_1.isGhooks(hook)) {
            console.log(`migrating existing ghooks script: ${name}`);
            return writeHook(filename, script);
        }
        // Migrate
        if (is_1.isPreCommit(hook)) {
            console.log(`migrating existing pre-commit script: ${name}`);
            return writeHook(filename, script);
        }
        // Update
        if (is_1.isHusky(hook) || is_1.isYorkie(hook)) {
            return writeHook(filename, script);
        }
        // Skip
        console.log(`skipping existing user hook: ${name}`);
        return;
    }
    // Create hook if it doesn't exist
    writeHook(filename, script);
}
function createHooks(filenames, script) {
    filenames.forEach((filename) => createHook(filename, script));
}
function canRemove(filename) {
    if (fs_1.default.existsSync(filename)) {
        const data = fs_1.default.readFileSync(filename, 'utf-8');
        return is_1.isHusky(data);
    }
    return false;
}
function removeHook(filename) {
    fs_1.default.unlinkSync(filename);
}
function removeHooks(filenames) {
    filenames.filter(canRemove).forEach(removeHook);
}
// This prevents the case where someone would want to debug a node_module that has
// husky as devDependency and run npm install from node_modules directory
function isInNodeModules(dir) {
    // INIT_CWD holds the full path you were in when you ran npm install (supported also by yarn and pnpm)
    // See https://docs.npmjs.com/cli/run-script
    if (process.env.INIT_CWD) {
        return process.env.INIT_CWD.indexOf('node_modules') !== -1;
    }
    // Old technique
    return (dir.match(/node_modules/g) || []).length > 1;
}
function getGitHooksDir(gitDir) {
    return path_1.default.join(gitDir, 'hooks');
}
function getHooks(gitDir) {
    const gitHooksDir = getGitHooksDir(gitDir);
    return hookList.map((hookName) => path_1.default.join(gitHooksDir, hookName));
}
/**
 * @param topLevel - as returned by git --rev-parse
 * @param gitDir - as returned by git --rev-parse
 * @param huskyDir - e.g. /home/typicode/project/node_modules/husky/
 * @param isCI - true if running in CI
 * @param requireRunNodePath - path to run-node resolved by require e.g. /home/typicode/project/node_modules/run-node/run-node
 */
// eslint-disable-next-line max-params
function install(topLevel, gitDir, huskyDir, isCI, requireRunNodePath = require.resolve('run-node/run-node')) {
    // First directory containing user's package.json
    const userPkgDir = pkg_dir_1.default.sync(path_1.default.join(huskyDir, '..'));
    if (userPkgDir === undefined) {
        console.log("Can't find package.json, skipping Git hooks installation.");
        console.log('Please check that your project has a package.json or create it and reinstall husky.');
        return;
    }
    // Get conf from package.json or .huskyrc
    const conf = getConf_1.default(userPkgDir);
    // Checks
    if (isCI && conf.skipCI) {
        console.log('CI detected, skipping Git hooks installation.');
        return;
    }
    if (isInNodeModules(huskyDir)) {
        console.log('Trying to install from node_modules directory, skipping Git hooks installation.');
        return;
    }
    // Create hooks directory if it doesn't exist
    const gitHooksDir = getGitHooksDir(gitDir);
    if (!fs_1.default.existsSync(gitHooksDir)) {
        fs_1.default.mkdirSync(gitHooksDir);
    }
    const hooks = getHooks(gitDir);
    const script = getScript_1.default(topLevel, huskyDir, requireRunNodePath);
    createHooks(hooks, script);
}
exports.install = install;
function uninstall(gitDir, huskyDir) {
    if (gitDir === null) {
        console.log("Can't find resolved .git directory, skipping Git hooks uninstallation.");
        return;
    }
    if (isInNodeModules(huskyDir)) {
        console.log('Trying to uninstall from node_modules directory, skipping Git hooks uninstallation.');
        return;
    }
    // Remove hooks
    const hooks = getHooks(gitDir);
    removeHooks(hooks);
}
exports.uninstall = uninstall;
