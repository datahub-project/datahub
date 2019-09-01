"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chalk_1 = __importDefault(require("chalk"));
const is_ci_1 = __importDefault(require("is-ci"));
const path_1 = __importDefault(require("path"));
const debug_1 = __importDefault(require("../debug"));
const _1 = require("./");
const gitRevParse_1 = __importDefault(require("./gitRevParse"));
// Debug
debug_1.default(`cwd: ${process.cwd()}`);
debug_1.default(`INIT_CWD: ${process.env.INIT_CWD}`);
// Action can be "install" or "uninstall"
// huskyDir is ONLY used in dev, don't use this arguments
const [, , action, huskyDir = path_1.default.join(__dirname, '../..')] = process.argv;
// Find Git dir
try {
    // Show un/install message
    console.log('husky > %s git hooks', action === 'install' ? 'Setting up' : 'Uninstalling');
    // Skip install if HUSKY_SKIP_INSTALL=1
    if (action === 'install' &&
        ['1', 'true'].includes(process.env.HUSKY_SKIP_INSTALL || '')) {
        console.log("HUSKY_SKIP_INSTALL environment variable is set to 'true',", 'skipping Git hooks installation.');
        process.exit(0);
    }
    // Get top level and git dir
    const { topLevel, absoluteGitDir } = gitRevParse_1.default();
    // Debug
    debug_1.default(`topLevel: ${topLevel}`);
    debug_1.default(`gitDir: ${absoluteGitDir}`);
    // Install or uninstall
    if (action === 'install') {
        _1.install(topLevel, absoluteGitDir, huskyDir, is_ci_1.default);
    }
    else {
        _1.uninstall(absoluteGitDir, huskyDir);
    }
    console.log(`husky > Done`);
}
catch (error) {
    console.log(chalk_1.default.red(error.message.trim()));
    console.log(chalk_1.default.red(`husky > Failed to ${action}`));
}
