import { describe, expect, it } from 'vitest';

import {
    formatCode,
    isFormatCodeSuccess,
    resolveCodeLanguage,
    resolveSqlFormatterLanguage,
} from '@components/components/CodeBlock/formatCode';

describe('resolveCodeLanguage', () => {
    it('should map dialects onto canonical highlighter ids', () => {
        expect(resolveCodeLanguage('Snowflake')).toBe('sql');
        expect(resolveCodeLanguage('yml')).toBe('yaml');
        expect(resolveCodeLanguage('json')).toBe('json');
        expect(resolveCodeLanguage('gql')).toBe('graphql');
        expect(resolveCodeLanguage('proto')).toBe('protobuf');
        expect(resolveCodeLanguage('py')).toBe('python');
        expect(resolveCodeLanguage('shell')).toBe('bash');
        expect(resolveCodeLanguage('jsonschema')).toBe('json');
    });
});

describe('resolveSqlFormatterLanguage', () => {
    it('should map dialects onto sql-formatter languages', () => {
        expect(resolveSqlFormatterLanguage('sql')).toBe('sql');
        expect(resolveSqlFormatterLanguage('ansi')).toBe('sql');
        expect(resolveSqlFormatterLanguage('Snowflake')).toBe('snowflake');
        expect(resolveSqlFormatterLanguage('databricks')).toBe('spark');
        expect(resolveSqlFormatterLanguage('json')).toBeUndefined();
    });
});

describe('formatCode', () => {
    it('should pretty-print JSON', () => {
        const result = formatCode('{"a":1,"b":2}', 'json');
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toBe('{\n  "a": 1,\n  "b": 2\n}\n');
        }
    });

    it('should reject invalid JSON', () => {
        expect(formatCode('{', 'json')).toEqual({ error: 'invalid' });
    });

    it('should pretty-print SQL', () => {
        const result = formatCode(
            "SELECT    order_id,   SUM(amount) AS total_revenue   \nFROM analytics.orders\n\n\nWHERE status = 'completed'",
            'sql',
        );
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toBe(
                "SELECT\n  order_id,\n  SUM(amount) AS total_revenue\nFROM\n  analytics.orders\nWHERE\n  status = 'completed'\n",
            );
        }
    });

    it('should pretty-print Snowflake SQL using that dialect', () => {
        const result = formatCode(
            'select a from t qualify row_number() over (partition by a order by b desc) = 1',
            'snowflake',
        );
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toContain('QUALIFY');
            expect(result.formatted).toContain('ROW_NUMBER()');
        }
    });

    it('should pretty-print YAML', () => {
        const result = formatCode(
            `name:    orders_revenue
type:   metric


sql: |
  SELECT SUM(amount) FROM orders
tags: [finance,  prod]`,
            'yaml',
        );
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toBe(
                'name: orders_revenue\ntype: metric\nsql: |\n  SELECT SUM(amount) FROM orders\ntags:\n  - finance\n  - prod\n',
            );
        }
    });

    it('should reject invalid YAML', () => {
        expect(formatCode('name: [unterminated', 'yaml')).toEqual({ error: 'invalid' });
    });

    it('should pretty-print GraphQL SDL', () => {
        const result = formatCode(
            'type Query { user(id: ID!): User }  type User { name:String  email: String }',
            'graphql',
        );
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toBe(
                'type Query {\n  user(id: ID!): User\n}\n\ntype User {\n  name: String\n  email: String\n}\n',
            );
        }
    });

    it('should pretty-print GraphQL when the language id is gql', () => {
        const result = formatCode('{ user { name } }', 'gql');
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toContain('user {');
            expect(result.formatted).toContain('name');
        }
    });

    it('should reject invalid GraphQL', () => {
        expect(formatCode('type Query {', 'graphql')).toEqual({ error: 'invalid' });
    });

    it('should tidy trailing whitespace for other languages', () => {
        const result = formatCode('function  foo()   {\n\n\n}', 'javascript');
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toBe('function  foo()   {\n\n}\n');
        }
    });

    it('should tidy protobuf without rewriting syntax', () => {
        const result = formatCode('message User {  \n\n\n  string name = 1;  \n}', 'protobuf');
        expect(isFormatCodeSuccess(result)).toBe(true);
        if (isFormatCodeSuccess(result)) {
            expect(result.formatted).toBe('message User {\n\n  string name = 1;\n}\n');
        }
    });

    it('should leave empty input unchanged', () => {
        expect(formatCode('', 'sql')).toEqual({ error: 'unchanged' });
        expect(formatCode('   ', 'json')).toEqual({ error: 'unchanged' });
    });
});
