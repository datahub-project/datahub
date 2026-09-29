import { describe, expect, it } from 'vitest';

import { resolveCodeLanguage } from '@components/components/CodeBlock/formatCode';
import {
    collectSqlValidateCodeDetails,
    composeValidateCodeMessage,
    formatSqlValidateCodeDetail,
    formatValidateCodeDetail,
    resolveSqlParserDatabase,
    splitSqlStatements,
    validateCode,
} from '@components/components/CodeBlock/validateCode';

describe('resolveSqlParserDatabase', () => {
    it('should map SQL dialects onto parser database names', () => {
        expect(resolveSqlParserDatabase('sql')).toBe('MySQL');
        expect(resolveSqlParserDatabase('postgresql')).toBe('PostgreSQL');
        expect(resolveSqlParserDatabase('snowflake')).toBe('Snowflake');
        expect(resolveSqlParserDatabase('json')).toBeUndefined();
    });
});

describe('formatValidateCodeDetail', () => {
    it('should prefer line, column, and unexpected token', () => {
        expect(
            formatValidateCodeDetail({
                message: 'Expected "(" but "o" found.',
                found: 'o',
                location: { start: { line: 1, column: 13 } },
            }),
        ).toBe('near line 1, column 13 — unexpected "o"');
    });

    it('should fall back to the raw message when location is missing', () => {
        expect(formatValidateCodeDetail(new Error('Expected property name'))).toBe('Expected property name');
    });
});

describe('formatSqlValidateCodeDetail', () => {
    it('should explain a missing SELECT list instead of blaming the table name', () => {
        expect(
            formatSqlValidateCodeDetail('SELECT\n\nFROM\n  orders', {
                found: 'o',
                location: { start: { line: 4, column: 3 } },
            }),
        ).toBe('SELECT is missing columns — add * or a column list before FROM');
    });

    it('should explain a missing table after FROM', () => {
        expect(
            formatSqlValidateCodeDetail('SELECT * FROM', {
                found: null,
                location: { start: { line: 1, column: 14 } },
            }),
        ).toBe('FROM is missing a table name');
    });
});

describe('collectSqlValidateCodeDetails', () => {
    it('should collect multiple heuristic issues from one statement', () => {
        expect(
            collectSqlValidateCodeDetails('SELECT FROM', {
                found: null,
                location: { start: { line: 1, column: 12 } },
            }),
        ).toEqual(['SELECT is missing columns — add * or a column list before FROM', 'FROM is missing a table name']);
    });
});

describe('splitSqlStatements', () => {
    it('should split on semicolons and drop empties', () => {
        expect(splitSqlStatements('SELECT 1; SELECT 2;;')).toEqual(['SELECT 1', 'SELECT 2']);
    });
});

describe('composeValidateCodeMessage', () => {
    it('should append detail when present', () => {
        expect(composeValidateCodeMessage('This SQL may have a syntax problem.', 'near line 1, column 13')).toBe(
            'This SQL may have a syntax problem. near line 1, column 13.',
        );
        expect(composeValidateCodeMessage('This SQL may have a syntax problem.', undefined)).toBe(
            'This SQL may have a syntax problem.',
        );
    });
});

describe('validateCode', () => {
    it('should treat empty input as valid', async () => {
        expect(await validateCode('', 'sql')).toEqual({ valid: true });
        expect(await validateCode('   ', 'json')).toEqual({ valid: true });
    });

    it('should accept valid JSON and reject invalid JSON as an error', async () => {
        expect(await validateCode('{"a":1}', 'json')).toEqual({ valid: true });
        const invalid = await validateCode('{', 'json');
        expect(invalid.valid).toBe(false);
        if (!invalid.valid) {
            expect(invalid.severity).toBe('error');
            expect(invalid.detail).toMatch(/JSON|property|position/i);
        }
    });

    it('should accept valid YAML and reject invalid YAML as an error', async () => {
        expect(await validateCode('name: demo', 'yaml')).toEqual({ valid: true });
        const invalid = await validateCode('name: [unterminated', 'yaml');
        expect(invalid.valid).toBe(false);
        if (!invalid.valid) {
            expect(invalid.severity).toBe('error');
            expect(invalid.detail).toBeTruthy();
        }
    });

    it('should accept valid GraphQL and reject invalid GraphQL as an error', async () => {
        expect(await validateCode('query { viewer { id } }', 'graphql')).toEqual({ valid: true });
        const invalid = await validateCode('type Query {', 'graphql');
        expect(invalid.valid).toBe(false);
        if (!invalid.valid) {
            expect(invalid.severity).toBe('error');
            expect(invalid.detail).toBeTruthy();
        }
    });

    it('should accept valid SQL and warn on broken SQL with a friendly detail', async () => {
        expect(await validateCode('SELECT 1', 'sql')).toEqual({ valid: true });
        const invalid = await validateCode('SELECT FROM orders', 'sql');
        expect(invalid.valid).toBe(false);
        if (!invalid.valid) {
            expect(invalid.severity).toBe('warning');
            expect(invalid.details).toContain('SELECT is missing columns — add * or a column list before FROM');
            expect(invalid.detail).toBe('SELECT is missing columns — add * or a column list before FROM');
        }
    });

    it('should collect issues across multiple SQL statements', async () => {
        const invalid = await validateCode('SELECT FROM orders; SELECT * FROM', 'sql');
        expect(invalid.valid).toBe(false);
        if (!invalid.valid) {
            expect(invalid.details).toEqual(
                expect.arrayContaining([
                    'SELECT is missing columns — add * or a column list before FROM',
                    'FROM is missing a table name',
                ]),
            );
        }
    });

    it('should skip validation for languages without a checker', async () => {
        expect(resolveCodeLanguage('python')).toBe('python');
        expect(await validateCode('def broken(', 'python')).toEqual({ valid: true });
    });
});
