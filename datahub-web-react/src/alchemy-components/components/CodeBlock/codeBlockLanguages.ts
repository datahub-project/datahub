/**
 * Prism ids the code block and the editor language menu can highlight.
 * `javascript`, `css`, and `markup` ship with refractor's core build.
 */
export const CODE_BLOCK_LANGUAGES = [
    'bash',
    'css',
    'graphql',
    'javascript',
    'json',
    'markdown',
    'markup',
    'protobuf',
    'python',
    'sql',
    'typescript',
    'yaml',
] as const;

/**
 * File extensions and short ids that share a registered grammar.
 * SQL dialects are mapped in formatCode before they reach the highlighter.
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
