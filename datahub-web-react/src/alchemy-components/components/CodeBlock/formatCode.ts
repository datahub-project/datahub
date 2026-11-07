import { parse as parseGraphqlDocument, print as printGraphqlDocument } from 'graphql';
import yaml from 'js-yaml';

export type FormatCodeSuccess = {
    formatted: string;
};

export type FormatCodeFailure = {
    /** `unavailable` means the formatter chunk failed to load. */
    error: 'invalid' | 'unchanged' | 'unavailable';
};

export type FormatCodeResult = FormatCodeSuccess | FormatCodeFailure;

const LANGUAGE_ALIASES: Record<string, string> = {
    ansi: 'sql',
    bigquery: 'sql',
    databricks: 'sql',
    redshift: 'sql',
    snowflake: 'sql',
    hive: 'sql',
    spark: 'sql',
    postgres: 'sql',
    postgresql: 'sql',
    mysql: 'sql',
    tsql: 'sql',
    transactsql: 'sql',
    yml: 'yaml',
    jsonschema: 'json',
    gql: 'graphql',
    graphqls: 'graphql',
    proto: 'protobuf',
    py: 'python',
    js: 'javascript',
    ts: 'typescript',
    sh: 'bash',
    shell: 'bash',
    md: 'markdown',
};

const SQL_FORMATTER_LANGUAGES = {
    sql: 'sql',
    ansi: 'sql',
    snowflake: 'snowflake',
    bigquery: 'bigquery',
    redshift: 'redshift',
    databricks: 'spark',
    spark: 'spark',
    hive: 'hive',
    postgres: 'postgresql',
    postgresql: 'postgresql',
    mysql: 'mysql',
    tsql: 'tsql',
    transactsql: 'transactsql',
} as const;

type SqlFormatterLanguage = (typeof SQL_FORMATTER_LANGUAGES)[keyof typeof SQL_FORMATTER_LANGUAGES];

type SqlFormatterModule = typeof import('sql-formatter');

/**
 * Caches a successful load and drops a rejected one so the next call can retry.
 * A failed chunk fetch must not stick for the rest of the page.
 *
 * @param load - Loads the module
 * @returns Loader that reuses the in-flight or successful promise
 */
export function cacheSuccessfulImport<T>(load: () => Promise<T>): () => Promise<T> {
    let pending: Promise<T> | null = null;
    return () => {
        if (!pending) {
            pending = load().catch((error: unknown) => {
                pending = null;
                throw error;
            });
        }
        return pending;
    };
}

/**
 * sql-formatter is only needed when the user formats SQL. A static import
 * puts the dialect bundle on the home and entity chunks.
 *
 * @returns sql-formatter module, cached after the first successful format
 */
const loadSqlFormatter = cacheSuccessfulImport<SqlFormatterModule>(() => import('sql-formatter'));

const SQL_FORMAT_OPTIONS = {
    tabWidth: 2,
    keywordCase: 'upper' as const,
    functionCase: 'upper' as const,
};

const YAML_DUMP_OPTIONS = {
    indent: 2,
    lineWidth: 120,
    noRefs: true,
    quotingType: '"' as const,
    sortKeys: false,
};

/**
 * Maps dialect / alias ids onto a canonical highlighter language.
 *
 * @param language - Prism id or dialect such as `snowflake`
 * @returns Canonical language id used for highlighting and formatting
 */
export function resolveCodeLanguage(language: string): string {
    const normalized = language.trim().toLowerCase();
    return Object.hasOwn(LANGUAGE_ALIASES, normalized) ? LANGUAGE_ALIASES[normalized] : normalized;
}

/**
 * Maps a CodeBlock language / dialect onto a sql-formatter dialect, if any.
 *
 * @param language - Prism id or dialect such as `snowflake`
 * @returns sql-formatter language id, or undefined when this is not SQL
 */
export function resolveSqlFormatterLanguage(language: string): SqlFormatterLanguage | undefined {
    const normalized = language.trim().toLowerCase();
    if (!Object.hasOwn(SQL_FORMATTER_LANGUAGES, normalized)) {
        return undefined;
    }
    return SQL_FORMATTER_LANGUAGES[normalized as keyof typeof SQL_FORMATTER_LANGUAGES];
}

/**
 * Returns whether a format result replaced the original source.
 *
 * @param result - Outcome of {@link formatCode}
 * @returns True when `formatted` source is present
 */
export function isFormatCodeSuccess(result: FormatCodeResult): result is FormatCodeSuccess {
    return 'formatted' in result;
}

function withTrailingNewline(code: string): string {
    return code.endsWith('\n') ? code : `${code}\n`;
}

function formatJson(code: string): FormatCodeResult {
    try {
        const formatted = withTrailingNewline(JSON.stringify(JSON.parse(code), null, 2));
        if (formatted === code) {
            return { error: 'unchanged' };
        }
        return { formatted };
    } catch {
        return { error: 'invalid' };
    }
}

function formatYaml(code: string): FormatCodeResult {
    try {
        const loaded = yaml.load(code);
        if (loaded === undefined) {
            return { error: 'unchanged' };
        }
        const formatted = withTrailingNewline(yaml.dump(loaded, YAML_DUMP_OPTIONS).trimEnd());
        if (formatted === code) {
            return { error: 'unchanged' };
        }
        return { formatted };
    } catch {
        return { error: 'invalid' };
    }
}

async function formatSql(code: string, sqlLanguage: SqlFormatterLanguage): Promise<FormatCodeResult> {
    let sqlFormatter: SqlFormatterModule;
    try {
        sqlFormatter = await loadSqlFormatter();
    } catch {
        return { error: 'unavailable' };
    }
    const { format: formatSqlQuery } = sqlFormatter;
    try {
        const formatted = withTrailingNewline(
            formatSqlQuery(code, {
                language: sqlLanguage,
                ...SQL_FORMAT_OPTIONS,
            }),
        );
        if (formatted === code) {
            return { error: 'unchanged' };
        }
        return { formatted };
    } catch {
        return { error: 'invalid' };
    }
}

function formatGraphql(code: string): FormatCodeResult {
    try {
        const formatted = withTrailingNewline(printGraphqlDocument(parseGraphqlDocument(code)).trimEnd());
        if (formatted === code) {
            return { error: 'unchanged' };
        }
        return { formatted };
    } catch {
        return { error: 'invalid' };
    }
}

function tidyWhitespace(code: string): string {
    return withTrailingNewline(
        code
            .split('\n')
            .map((line) => line.trimEnd())
            .join('\n')
            .replace(/\n{3,}/g, '\n\n')
            .replace(/^\n+/, '')
            .replace(/\s+$/, ''),
    );
}

/**
 * Formats source for the writable CodeBlock.
 * SQL (including dialects), JSON, YAML, and GraphQL are pretty-printed;
 * other languages (protobuf, Python, JS/TS, bash) get whitespace cleanup.
 *
 * @param code - Current editor value
 * @param language - Prism / dialect id (e.g. `sql`, `snowflake`, `json`, `yaml`, `graphql`)
 * @returns Formatted source, `unchanged`, `invalid` when parsing fails, or `unavailable` when the SQL formatter could not load
 */
export async function formatCode(code: string, language: string): Promise<FormatCodeResult> {
    if (!code.trim()) {
        return { error: 'unchanged' };
    }
    const resolved = resolveCodeLanguage(language);
    if (resolved === 'json') {
        return formatJson(code);
    }
    if (resolved === 'yaml') {
        return formatYaml(code);
    }
    if (resolved === 'graphql') {
        return formatGraphql(code);
    }
    const sqlLanguage = resolveSqlFormatterLanguage(language);
    if (sqlLanguage) {
        return formatSql(code, sqlLanguage);
    }
    const formatted = tidyWhitespace(code);
    if (formatted === code) {
        return { error: 'unchanged' };
    }
    return { formatted };
}
