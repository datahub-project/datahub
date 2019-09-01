"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const slash_1 = __importDefault(require("slash"));
const execa_1 = __importDefault(require("execa"));
function default_1() {
    const result = execa_1.default.sync('git', [
        'rev-parse',
        '--show-toplevel',
        '--absolute-git-dir'
    ]);
    const [topLevel, absoluteGitDir] = result.stdout
        .trim()
        .split('\n')
        // Normalize for Windows
        .map(slash_1.default);
    // Git rev-parse returns unknown options as is.
    // If we get --absolute-git-dir in the output,
    // it probably means that an older version of Git has been used.
    if (absoluteGitDir === '--absolute-git-dir') {
        throw new Error('Husky requires Git >= 2.13.2, please update Git');
    }
    return { topLevel, absoluteGitDir };
}
exports.default = default_1;
