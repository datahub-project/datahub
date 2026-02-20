/**
 * ESLint rule: no-hardcoded-colors
 *
 * Disallows hardcoded color values in source files, including:
 *   - Hex colors: #fff, #ffffff, #ffffffAA
 *   - RGB/RGBA: rgb(0,0,0), rgba(0,0,0,0.5)
 *   - HSL/HSLA: hsl(0,0%,0%), hsla(0,0%,0%,0.5)
 *   - Named CSS colors used as property values: color: white, background: black
 *
 * All colors should use semantic tokens via styled-components theming:
 *   - In styled-components: ${(props) => props.theme.colors.<token>}
 *   - In component bodies: const theme = useTheme(); theme.colors.<token>
 *
 * See src/conf/theme/colorThemes/types.ts for available semantic tokens.
 */

const HEX_COLOR_REGEX = /#(?:[0-9a-fA-F]{3,4}){1,2}\b/g;
const RGB_RGBA_REGEX = /\brgba?\s*\(\s*\d/g;
const HSL_HSLA_REGEX = /\bhsla?\s*\(\s*\d/g;

// CSS named colors that appear in the codebase as hardcoded values.
// Only matches when used as a CSS property value (e.g., "color: white")
// to avoid false positives on words like "white-space" or text content.
// NOTE: "transparent", "inherit", "currentColor", "initial", "unset" are
// excluded — they are CSS keywords, not theme-dependent colors.
const CSS_COLOR_NAMES = [
    'white',
    'black',
    'red',
    'blue',
    'green',
    'gray',
    'grey',
    'orange',
    'yellow',
    'purple',
    'pink',
    'cyan',
];
const CSS_NAMED_COLOR_REGEX = new RegExp(
    `(?:color|background|background-color|border-color|fill|stroke|outline-color):\\s*(?:${CSS_COLOR_NAMES.join('|')})\\b`,
    'gi',
);

const MESSAGE =
    'Hardcoded color "{{color}}". Use semantic tokens: `${(props) => props.theme.colors.*}` in styled-components or `useTheme().colors.*` in components. See colorThemes/types.ts for available tokens.';

function findMatches(regex, value) {
    const results = [];
    regex.lastIndex = 0;
    let match = regex.exec(value);
    while (match !== null) {
        results.push(match[0]);
        match = regex.exec(value);
    }
    return results;
}

module.exports = {
    meta: {
        type: 'suggestion',
        docs: {
            description:
                'Disallow hardcoded color values. Use theme.colors.* semantic tokens via styled-components theming instead.',
        },
        schema: [],
        messages: {
            noHardcodedColor: MESSAGE,
        },
    },
    create(context) {
        function checkValue(node, value) {
            if (typeof value !== 'string') return;

            const allMatches = [
                ...findMatches(HEX_COLOR_REGEX, value),
                ...findMatches(RGB_RGBA_REGEX, value),
                ...findMatches(HSL_HSLA_REGEX, value),
                ...findMatches(CSS_NAMED_COLOR_REGEX, value),
            ];

            allMatches.forEach((color) => {
                context.report({
                    node,
                    messageId: 'noHardcodedColor',
                    data: { color },
                });
            });
        }

        return {
            Literal(node) {
                if (typeof node.value === 'string') {
                    checkValue(node, node.value);
                }
            },
            TemplateLiteral(node) {
                node.quasis.forEach((quasi) => {
                    checkValue(quasi, quasi.value.raw);
                });
            },
        };
    },
};
