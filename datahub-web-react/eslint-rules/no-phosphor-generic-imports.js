/**
 * ESLint rule: no-phosphor-generic-imports
 *
 * Enforces that Phosphor icons must be imported from specific icon paths (not generic folders):
 * - ❌ import { Icon } from '@phosphor-icons/react'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist/csr'
 * - ❌ import { Icon } from '@phosphor-icons/react/dist/ssr'
 * - ✅ import { Icon } from '@phosphor-icons/react/dist/csr/IconName'
 * - ✅ import { Icon } from '@phosphor-icons/react/dist/ssr/IconName'
 * - ✅ import type { Icon } from '@phosphor-icons/react'
 */

function isPhosphorSource(value) {
    return typeof value === 'string' && value.startsWith('@phosphor-icons/react');
}

function checkPhosphorImport(node, context) {
    if (!node || typeof node.value !== 'string') return;
    if (!isPhosphorSource(node.value)) return;

    const source = node.value;
    const parent = node.parent || {};

    // Allow: import type { Icon } from '@phosphor-icons/react'
    if (parent.type === 'ImportDeclaration' && parent.importKind === 'type') {
        return;
    }

    const blockMessage =
        'Import Phosphor icons from individual CSR paths: @phosphor-icons/react/dist/csr/IconName.';

    // Block: '@phosphor-icons/react' or '@phosphor-icons/react/' (root)
    if (source === '@phosphor-icons/react' || source === '@phosphor-icons/react/') {
        context.report({ node, message: blockMessage });
    }
    // Block: '@phosphor-icons/react/dist' or '@phosphor-icons/react/dist/' (generic dist)
    else if (source === '@phosphor-icons/react/dist' || source === '@phosphor-icons/react/dist/') {
        context.report({ node, message: blockMessage });
    }
    // Block: '@phosphor-icons/react/dist/ssr' or '@phosphor-icons/react/dist/ssr/' (generic SSR folder)
    else if (source === '@phosphor-icons/react/dist/ssr' || source === '@phosphor-icons/react/dist/ssr/') {
        context.report({ node, message: blockMessage });
    }
    // Block: '@phosphor-icons/react/dist/csr' or '@phosphor-icons/react/dist/csr/' (generic CSR folder)
    else if (source === '@phosphor-icons/react/dist/csr' || source === '@phosphor-icons/react/dist/csr/') {
        context.report({ node, message: blockMessage });
    }
    // Allow: '@phosphor-icons/react/dist/csr/IconName' and '@phosphor-icons/react/dist/ssr/IconName'
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
