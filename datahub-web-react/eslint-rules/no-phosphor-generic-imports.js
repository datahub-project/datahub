/**
 * ESLint rule: no-phosphor-generic-imports
 *
 * Enforces that Phosphor icons must be imported from specific icon paths (allow-list pattern):
 * - ❌ import { Icon } from '@phosphor-icons/react'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist/csr'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist/ssr'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist/lib'
 * - ✅ import { Icon } from '@phosphor-icons/react/dist/csr/IconName'
 * - ✅ import { Icon } from '@phosphor-icons/react/dist/ssr/IconName'
 * - ✅ import { Icon } from '@phosphor-icons/react/dist/lib/types' (type definitions)
 * - ✅ import type { Icon } from '@phosphor-icons/react'
 */

const BLOCK_MESSAGE =
    'Import Phosphor icons from individual icon paths: @phosphor-icons/react/dist/csr/IconName or @phosphor-icons/react/dist/ssr/IconName.';

// Allow only specific icon imports (csr/ssr/IconName), excluding index and other special files
const ALLOWED_ICON_PATTERN = /^@phosphor-icons\/react\/dist\/(csr|ssr)\/(?!index)\w+$/;

function checkPhosphorImport(node, context) {
    if (!node?.value || typeof node.value !== 'string') return;

    const source = node.value;
    if (!source.startsWith('@phosphor-icons/react')) return;

    const parent = node.parent || {};

    // Allow: import type { Icon } from '@phosphor-icons/react'
    if (parent.type === 'ImportDeclaration' && parent.importKind === 'type') {
        return;
    }

    // Allow: type imports from /dist/lib/types (type definitions)
    if (source.includes('/dist/lib/types')) {
        return;
    }

    // Allow: specific icon imports matching @phosphor-icons/react/dist/(csr|ssr)/IconName
    if (ALLOWED_ICON_PATTERN.test(source)) {
        return;
    }

    // Block everything else
    context.report({ node, message: BLOCK_MESSAGE });
}

module.exports = {
    meta: {
        type: 'suggestion',
        docs: {
            description:
                'Enforce Phosphor icon imports from specific icon paths, not generic folders.',
        },
    },
    create(context) {
        return {
            ImportDeclaration(node) {
                checkPhosphorImport(node.source, context);
            },
            ExportNamedDeclaration(node) {
                if (node.source) {
                    checkPhosphorImport(node.source, context);
                }
            },
            ExportAllDeclaration(node) {
                checkPhosphorImport(node.source, context);
            },
            ImportExpression(node) {
                if (node.source && node.source.type === 'Literal') {
                    checkPhosphorImport(node.source, context);
                }
            },
            CallExpression(node) {
                if (
                    node.callee.type === 'Identifier' &&
                    node.callee.name === 'require' &&
                    node.arguments.length === 1 &&
                    node.arguments[0].type === 'Literal'
                ) {
                    checkPhosphorImport(node.arguments[0], context);
                }
            },
        };
    },
};
