/**
 * Disallows background semantic tokens in CSS border declarations.
 *
 * Background and border tokens can resolve similarly in light mode but diverge
 * in dark mode, producing invisible or unexpectedly dark borders.
 */

const BORDER_PROPERTY_REGEX = /^(?:border(?:Top|Right|Bottom|Left)?(?:Color)?|outlineColor)$/;
const CSS_BORDER_CONTEXT_REGEX =
    /(?:^|[;{]\s*)(?:border(?:-(?:top|right|bottom|left))?(?:-color)?|outline-color)\s*:[^;]*$/i;

function getStaticPropertyName(node) {
    if (!node?.computed && node?.property?.type === 'Identifier') return node.property.name;
    if (node?.computed && node?.property?.type === 'Literal') return node.property.value;
    return undefined;
}

function isBackgroundThemeToken(node) {
    if (node.type !== 'MemberExpression') return false;

    const token = getStaticPropertyName(node);
    const colorsExpression = node.object;
    return (
        typeof token === 'string' &&
        token.startsWith('bg') &&
        colorsExpression?.type === 'MemberExpression' &&
        getStaticPropertyName(colorsExpression) === 'colors'
    );
}

function isBorderObjectProperty(node) {
    let value = node;
    while (
        value.parent &&
        ['ConditionalExpression', 'LogicalExpression', 'TemplateLiteral', 'TSAsExpression'].includes(value.parent.type)
    ) {
        value = value.parent;
    }

    const property = value.parent;
    if (property?.type !== 'Property' || property.value !== value) return false;

    const key = property.key?.name ?? property.key?.value;
    return typeof key === 'string' && BORDER_PROPERTY_REGEX.test(key);
}

function isBorderJsxAttribute(node) {
    let value = node;
    while (value.parent && value.parent.type !== 'JSXExpressionContainer') {
        value = value.parent;
    }

    const attribute = value.parent?.parent;
    return attribute?.type === 'JSXAttribute' && BORDER_PROPERTY_REGEX.test(attribute.name?.name);
}

function isBorderTemplateExpression(node) {
    let expression = node;
    while (expression.parent && expression.parent.type !== 'TemplateLiteral') {
        expression = expression.parent;
    }

    const template = expression.parent;
    if (template?.type !== 'TemplateLiteral') return false;

    const expressionIndex = template.expressions.indexOf(expression);
    if (expressionIndex < 0) return false;

    return CSS_BORDER_CONTEXT_REGEX.test(template.quasis[expressionIndex].value.raw);
}

module.exports = {
    meta: {
        type: 'problem',
        docs: {
            description: 'Disallow background semantic tokens in CSS border declarations.',
        },
        schema: [],
        messages: {
            backgroundTokenInBorder:
                'Background token "{{token}}" is used as a border color. Use a border* semantic token, or use transparent when the border should be invisible.',
        },
    },
    create(context) {
        return {
            MemberExpression(node) {
                if (!isBackgroundThemeToken(node)) return;
                if (!isBorderObjectProperty(node) && !isBorderJsxAttribute(node) && !isBorderTemplateExpression(node)) {
                    return;
                }

                context.report({
                    node,
                    messageId: 'backgroundTokenInBorder',
                    data: { token: getStaticPropertyName(node) },
                });
            },
        };
    },
};
