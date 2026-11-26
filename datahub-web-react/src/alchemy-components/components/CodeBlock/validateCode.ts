import { parse as parseGraphqlDocument } from 'graphql';
import yaml from 'js-yaml';

import { resolveCodeLanguage, resolveSqlFormatterLanguage } from '@components/components/CodeBlock/formatCode';

export type ValidateCodeSeverity = 'error' | 'warning';

export type ValidateCodeValid = {
    valid: true;
};

export type ValidateCodeInvalid = {
    valid: false;
    severity: ValidateCodeSeverity;
    /**
     * All distinct human-readable hints (location / unexpected token / heuristics).
     * Prefer this over {@link detail} when rendering a list.
     */
    details: string[];
    /** First entry of {@link details}, kept for callers that only need one line. */
    detail?: string;
};

export type ValidateCodeResult = ValidateCodeValid | ValidateCodeInvalid;

type ParseErrorLike = {
    message?: string;
    found?: string | null;
    location?: {
        start?: {
            line?: number;
            column?: number;
        };
    };
};

/**
 * node-sql-parser `database` option for each CodeBlock language / dialect id.
 * Unknown SQL dialects fall back to MySQL (parser default).
 *
 * Loaded via dynamic `import('node-sql-parser')` (same pattern as mermaid) and
 * kept out of the eager vendor chunk in vite.config — so Validate stays strong
 * without bloating the initial app bundle or OOMing CI builds.
 */
const SQL_PARSER_DATABASES: Record<string, string> = {
    sql: 'MySQL',
    ansi: 'MySQL',
    mysql: 'MySQL',
    postgres: 'PostgreSQL',
    postgresql: 'PostgreSQL',
    snowflake: 'Snowflake',
    bigquery: 'BigQuery',
    redshift: 'Redshift',
    hive: 'Hive',
    spark: 'Hive',
    databricks: 'Hive',
    tsql: 'TransactSQL',
    transactsql: 'TransactSQL',
};

type SqlParserModule = typeof import('node-sql-parser');

let sqlParserModulePromise: Promise<SqlParserModule> | null = null;

/**
 * Dynamically loads the SQL parser (cached after first Validate).
 *
 * @returns node-sql-parser module
 */
function loadSqlParser(): Promise<SqlParserModule> {
    if (!sqlParserModulePromise) {
        sqlParserModulePromise = import('node-sql-parser');
    }
    return sqlParserModulePromise;
}

/**
 * Maps a CodeBlock language id onto a node-sql-parser database name.
 *
 * @param language - Prism / dialect id
 * @returns Parser database option, or undefined when this is not SQL
 */
export function resolveSqlParserDatabase(language: string): string | undefined {
    if (!resolveSqlFormatterLanguage(language)) {
        return undefined;
    }
    const normalized = language.trim().toLowerCase();
    return SQL_PARSER_DATABASES[normalized] ?? 'MySQL';
}

/**
 * Turns a raw parser exception into a short, user-facing hint.
 * Prefers line/column + unexpected token over the full PEG expected-list.
 *
 * @param error - Caught parse exception
 * @returns Compact detail string, or undefined when nothing useful is available
 */
export function formatValidateCodeDetail(error: unknown): string | undefined {
    if (error == null) {
        return undefined;
    }

    const err = error as ParseErrorLike;
    const line = err.location?.start?.line;
    const column = err.location?.start?.column;
    const parts: string[] = [];

    if (typeof line === 'number' && typeof column === 'number') {
        parts.push(`near line ${line}, column ${column}`);
    }

    if (err.found === null) {
        parts.push('unexpected end of input');
    } else if (typeof err.found === 'string' && err.found.length > 0) {
        parts.push(`unexpected ${JSON.stringify(err.found)}`);
    } else if (typeof err.message === 'string' && err.message.trim()) {
        // JSON / YAML / GraphQL messages are already fairly readable.
        parts.push(err.message.trim());
    } else if (error instanceof Error && error.message.trim()) {
        parts.push(error.message.trim());
    }

    return parts.length > 0 ? parts.join(' — ') : undefined;
}

/**
 * Strips SQL comments so structural heuristics can run on the remaining text.
 *
 * @param code - Raw SQL
 * @returns Code with block/line comments replaced by spaces
 */
function stripSqlComments(code: string): string {
    return code.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/--[^\n]*/g, ' ');
}

/**
 * Collects friendly SQL hints for common broken shapes, then the parser detail
 * when it adds something new.
 *
 * @param code - SQL that failed to parse (one statement or whole script)
 * @param error - Caught parse exception
 * @returns Distinct user-facing detail strings (may be empty)
 */
export function collectSqlValidateCodeDetails(code: string, error: unknown): string[] {
    const normalized = stripSqlComments(code);
    const details: string[] = [];

    // `SELECT FROM …` — missing select list. The parser often blames the table
    // name ("unexpected o") because it treats FROM as an identifier first.
    if (/\bSELECT\s+FROM\b/i.test(normalized)) {
        details.push('SELECT is missing columns — add * or a column list before FROM');
    }

    // `SELECT * FROM` with nothing after — missing table.
    if (/\bFROM\s*$/i.test(normalized.trim())) {
        details.push('FROM is missing a table name');
    }

    const parserDetail = formatValidateCodeDetail(error);
    if (parserDetail && !details.includes(parserDetail)) {
        // Skip raw parser noise when a heuristic already explained the same break.
        const heuristicCoveredSelectFrom = details.some((d) => d.includes('SELECT is missing columns'));
        const looksLikeSelectFromNoise =
            heuristicCoveredSelectFrom && /unexpected\s+/i.test(parserDetail) && /line\s+\d+/i.test(parserDetail);
        if (!looksLikeSelectFromNoise) {
            details.push(parserDetail);
        }
    }

    return details;
}

/**
 * Maps common broken-SQL shapes onto clearer copy than the raw PEG message.
 * Falls back to {@link formatValidateCodeDetail} when no pattern matches.
 *
 * @param code - SQL that failed to parse
 * @param error - Caught parse exception
 * @returns User-facing detail string
 */
export function formatSqlValidateCodeDetail(code: string, error: unknown): string | undefined {
    return collectSqlValidateCodeDetails(code, error)[0];
}

/**
 * Splits a SQL script into statements on `;`, ignoring empty segments.
 * Semicolons inside quotes are not handled — good enough for validate UX.
 *
 * @param code - Raw SQL
 * @returns Non-empty trimmed statements
 */
export function splitSqlStatements(code: string): string[] {
    return code
        .split(';')
        .map((part) => part.trim())
        .filter((part) => part.length > 0);
}

function invalidResult(severity: ValidateCodeSeverity, error: unknown, details?: string[]): ValidateCodeInvalid {
    const resolvedDetails = details?.length
        ? details
        : (() => {
              const single = formatValidateCodeDetail(error);
              return single ? [single] : [];
          })();
    return {
        valid: false,
        severity,
        details: resolvedDetails,
        detail: resolvedDetails[0],
    };
}

function validateJson(code: string): ValidateCodeResult {
    try {
        JSON.parse(code);
        return { valid: true };
    } catch (error) {
        return invalidResult('error', error);
    }
}

function validateYaml(code: string): ValidateCodeResult {
    try {
        yaml.load(code);
        return { valid: true };
    } catch (error) {
        return invalidResult('error', error);
    }
}

function validateGraphql(code: string): ValidateCodeResult {
    try {
        parseGraphqlDocument(code);
        return { valid: true };
    } catch (error) {
        return invalidResult('error', error);
    }
}

async function validateSql(code: string, language: string): Promise<ValidateCodeResult> {
    const database = resolveSqlParserDatabase(language);
    if (!database) {
        return { valid: true };
    }

    const { Parser } = await loadSqlParser();
    const parser = new Parser();
    const statements = splitSqlStatements(code);
    const details = statements.flatMap((statement) => {
        try {
            parser.astify(statement, { database });
            return [];
        } catch (error) {
            return collectSqlValidateCodeDetails(statement, error);
        }
    });
    const uniqueDetails = details.filter((detail, index) => details.indexOf(detail) === index);

    if (uniqueDetails.length === 0) {
        return { valid: true };
    }
    return invalidResult('warning', undefined, uniqueDetails);
}

/**
 * Checks whether source parses for the given language.
 * JSON / YAML / GraphQL fail as errors; SQL fails as a soft warning.
 * Empty input is treated as valid (requiredness is a parent concern).
 *
 * @param code - Current editor value
 * @param language - Prism / dialect id
 * @returns Parse outcome with severity and optional detail when invalid
 */
export async function validateCode(code: string, language: string): Promise<ValidateCodeResult> {
    if (!code.trim()) {
        return { valid: true };
    }
    const resolved = resolveCodeLanguage(language);
    if (resolved === 'json') {
        return validateJson(code);
    }
    if (resolved === 'yaml') {
        return validateYaml(code);
    }
    if (resolved === 'graphql') {
        return validateGraphql(code);
    }
    if (resolveSqlFormatterLanguage(language)) {
        return validateSql(code, language);
    }
    return { valid: true };
}

/**
 * Whether a validate result should use the hard-error chrome.
 *
 * @param result - Outcome of {@link validateCode}
 * @returns True when severity is `error`
 */
export function isValidateCodeError(result: ValidateCodeResult): boolean {
    return !result.valid && result.severity === 'error';
}

/**
 * Combines the generic status copy with an optional parser detail.
 *
 * @param baseMessage - Localized generic warning/error
 * @param detail - Optional specific hint from {@link formatValidateCodeDetail}
 * @returns Message shown under the CodeBlock
 */
export function composeValidateCodeMessage(baseMessage: string, detail?: string): string {
    const trimmedDetail = detail?.trim();
    if (!trimmedDetail) {
        return baseMessage;
    }
    return `${baseMessage} ${trimmedDetail}.`;
}
