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

// Match exact type definition entrypoint
const TYPE_DEFS_PATTERN = /^@phosphor-icons\/react\/dist\/lib\/types(\/.*)?$/;

function extractStringValue(sourceNode) {
    if (!sourceNode) return null;

    // Handle Literal nodes (string literals)
    if (sourceNode.type === 'Literal' && typeof sourceNode.value === 'string') {
        return sourceNode.value;
    }

    // Handle TemplateLiteral nodes (backtick strings) with no expressions
    if (sourceNode.type === 'TemplateLiteral' && sourceNode.expressions.length === 0) {
        return sourceNode.quasis[0]?.value?.cooked || null;
    }

    return null;
}

function checkPhosphorImport(sourceValue, sourceNode, context) {
    if (!sourceValue || typeof sourceValue !== 'string') return;

    if (!sourceValue.startsWith('@phosphor-icons/react')) return;

    const parent = sourceNode.parent || {};

    // Allow: import type { Icon } from '@phosphor-icons/react'
    // Allow: export type { Icon } from '@phosphor-icons/react'
    if (parent.importKind === 'type' || parent.exportKind === 'type') {
        return;
    }

    // Allow: exact type definition entrypoint only
    if (TYPE_DEFS_PATTERN.test(sourceValue)) {
        return;
    }

    // Allow: specific icon imports matching @phosphor-icons/react/dist/(csr|ssr)/IconName
    if (ALLOWED_ICON_PATTERN.test(sourceValue)) {
        return;
    }

    // Block everything else
    context.report({ node: sourceNode, message: BLOCK_MESSAGE });
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
                checkPhosphorImport(node.source.value, node.source, context);
            },
            ExportNamedDeclaration(node) {
                if (node.source) {
                    checkPhosphorImport(node.source.value, node.source, context);
                }
            },
            ExportAllDeclaration(node) {
                checkPhosphorImport(node.source.value, node.source, context);
            },
            ImportExpression(node) {
                const sourceValue = extractStringValue(node.source);
                if (sourceValue) {
                    checkPhosphorImport(sourceValue, node.source, context);
                }
            },
            CallExpression(node) {
                if (
                    node.callee.type === 'Identifier' &&
                    node.callee.name === 'require' &&
                    node.arguments.length === 1
                ) {
                    const sourceValue = extractStringValue(node.arguments[0]);
                    if (sourceValue) {
                        checkPhosphorImport(sourceValue, node.arguments[0], context);
                    }
                }
            },
        };
    },
};
