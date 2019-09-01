"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const _1 = __importDefault(require("./"));
const debug_1 = __importDefault(require("../debug"));
// Debug
debug_1.default(`cwd: ${process.cwd()}`);
// Run hook
_1.default(process.argv)
    .then((status) => process.exit(status))
    .catch((err) => {
    console.log('Husky > unexpected error', err);
    process.exit(1);
});
