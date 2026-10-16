/**
 * File extensions and short ids that share a registered grammar.
 * SQL dialects are mapped in formatCode before they reach the highlighter.
 * `javascript`, `css`, and `markup` ship with refractor's core build.
 */
export const CODE_BLOCK_LANGUAGE_ALIASES: Record<string, readonly string[]> = {
    bash: ['sh', 'shell'],
    graphql: ['gql', 'graphqls'],
    javascript: ['js'],
    json: ['jsonschema'],
    markdown: ['md'],
    protobuf: ['proto'],
    python: ['py'],
    yaml: ['yml'],
};
