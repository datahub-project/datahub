/**
 * ESLint rule: no-antd-imports
 *
 * Disallows importing from `antd` or `antd/…`. New UI should use alchemy
 * components from `@components` instead.
 *
 * Severity is `error` in .eslintrc.js. Alchemy wrappers, tests, generated
 * GraphQL, and a leftover list of existing app files are excluded there.
 */

function isAntdSource(value) {
    return typeof value === 'string' && (value === 'antd' || value.startsWith('antd/'));
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
        schema: [],
        messages: {
            noAntdImport: MESSAGE,
        },
    },
    create(context) {
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
