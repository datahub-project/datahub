/**
 * ESLint rule: no-antd-imports
 *
 * Disallows importing from `antd` or `antd/…`. New UI should use alchemy
 * components from `@components` instead.
 *
 * Files that already imported antd on the baseline ref (origin/master, or
 * ANTD_IMPORT_BASELINE_REF / options.baselineRef) are grandfathered. 
 * Alchemy wrappers, tests, and generated GraphQL are excluded in .eslintrc.js.
 */

const { execFileSync } = require('child_process');
const path = require('path');

const ANTD_IMPORT_GREP =
    '(from|require\\(|import\\()[[:space:]]*[\'"]antd(/[^\'"]*)?[\'"]';

function isAntdSource(value) {
    return typeof value === 'string' && (value === 'antd' || value.startsWith('antd/'));
}

function toPosixAbs(filePath) {
    return path.resolve(filePath).replace(/\\/g, '/');
}

let gitBaselineCache;

function loadGitBaseline(baselineRef) {
    try {
        const toplevel = execFileSync('git', ['rev-parse', '--show-toplevel'], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore'],
        }).trim();

        const out = execFileSync(
            'git',
            ['grep', '-l', '-E', ANTD_IMPORT_GREP, baselineRef, '--', 'datahub-web-react/src'],
            {
                cwd: toplevel,
                encoding: 'utf8',
                maxBuffer: 10 * 1024 * 1024,
                stdio: ['ignore', 'pipe', 'ignore'],
            },
        );

        const prefix = `${baselineRef}:`;
        const files = new Set();
        out.split('\n').forEach((line) => {
            const rel = line.startsWith(prefix) ? line.slice(prefix.length) : line;
            if (!rel) return;
            files.add(toPosixAbs(path.join(toplevel, rel)));
        });
        return files;
    } catch (err) {
        // git grep exits 1 when the search succeeds but finds nothing.
        if (err && err.status === 1) {
            return new Set();
        }
        // Fail open otherwise: missing git, unknown ref, shallow clone, etc.
        return null;
    }
}

function getAllowedFiles(options) {
    if (Array.isArray(options.allowedFiles)) {
        return new Set(options.allowedFiles.map(toPosixAbs));
    }

    const baselineRef = options.baselineRef || process.env.ANTD_IMPORT_BASELINE_REF || 'origin/master';
    if (!gitBaselineCache || gitBaselineCache.ref !== baselineRef) {
        gitBaselineCache = { ref: baselineRef, files: loadGitBaseline(baselineRef) };
    }
    return gitBaselineCache.files;
}

function isGrandfathered(filename, allowed) {
    if (allowed === null) return true;
    if (!filename || filename === '<input>') return false;
    return allowed.has(toPosixAbs(filename));
}

const MESSAGE =
    "Don't import from '{{source}}'. Use alchemy components from `@components` instead.";

module.exports = {
    meta: {
        type: 'suggestion',
        docs: {
            description:
                'Disallow imports from antd / antd/*. Use alchemy components from @components instead.',
        },
        schema: [
            {
                type: 'object',
                properties: {
                    baselineRef: { type: 'string' },
                    allowedFiles: {
                        type: 'array',
                        items: { type: 'string' },
                    },
                },
                additionalProperties: false,
            },
        ],
        messages: {
            noAntdImport: MESSAGE,
        },
    },
    create(context) {
        const options = context.options[0] || {};
        const allowed = getAllowedFiles(options);
        if (allowed === null || isGrandfathered(context.getFilename(), allowed)) {
            return {};
        }

        function checkSource(node) {
            if (!node || typeof node.value !== 'string') return;
            if (!isAntdSource(node.value)) return;
            context.report({
                node,
                messageId: 'noAntdImport',
                data: { source: node.value },
            });
        }

        return {
            ImportDeclaration(node) {
                checkSource(node.source);
            },
            ExportNamedDeclaration(node) {
                checkSource(node.source);
            },
            ExportAllDeclaration(node) {
                checkSource(node.source);
            },
            ImportExpression(node) {
                if (node.source && node.source.type === 'Literal') {
                    checkSource(node.source);
                }
            },
            CallExpression(node) {
                if (
                    node.callee.type === 'Identifier' &&
                    node.callee.name === 'require' &&
                    node.arguments.length === 1 &&
                    node.arguments[0].type === 'Literal'
                ) {
                    checkSource(node.arguments[0]);
                }
            },
        };
    },
};
