import { describe, expect, it } from 'vitest';

import {
    getCodeBlockTestId,
    getLanguageControlMode,
    isLineHighlighted,
    mapLanguageOptionsToTabItems,
    mergeHighlightedLineProps,
    resolveLanguageLabel,
    resolveStaticLanguageLabel,
    shouldShowCodeBlockHeader,
    shouldShowLineNumbers,
} from '@components/components/CodeBlock/utils';

describe('CodeBlock utils', () => {
    describe('isLineHighlighted', () => {
        it('returns false when highlights are missing', () => {
            expect(isLineHighlighted(undefined, 1)).toBe(false);
        });

        it('supports array and set membership', () => {
            expect(isLineHighlighted([1, 3], 3)).toBe(true);
            expect(isLineHighlighted([1, 3], 2)).toBe(false);
            expect(isLineHighlighted(new Set([2]), 2)).toBe(true);
            expect(isLineHighlighted(new Set([2]), 1)).toBe(false);
        });
    });

    describe('shouldShowCodeBlockHeader', () => {
        it('shows when any header signal is present', () => {
            expect(
                shouldShowCodeBlockHeader({
                    showHeader: false,
                    showCopy: false,
                    languageOptions: [{ value: 'sql', label: 'SQL' }],
                }),
            ).toBe(true);
            expect(
                shouldShowCodeBlockHeader({
                    showHeader: false,
                    showCopy: false,
                    languageLabel: 'SQL',
                }),
            ).toBe(true);
            expect(
                shouldShowCodeBlockHeader({
                    showHeader: false,
                    showCopy: false,
                }),
            ).toBe(false);
        });
    });

    describe('resolveLanguageLabel', () => {
        it('respects explicit false and string labels', () => {
            expect(
                resolveLanguageLabel({
                    language: 'sql',
                    languageLabel: false,
                    hasHeaderLeft: false,
                }),
            ).toBeNull();
            expect(
                resolveLanguageLabel({
                    language: 'sql',
                    languageLabel: 'Custom',
                    hasHeaderLeft: false,
                }),
            ).toBe('Custom');
        });

        it('hides auto label when headerLeft owns the slot', () => {
            expect(
                resolveLanguageLabel({
                    language: 'sql',
                    hasHeaderLeft: true,
                }),
            ).toBeNull();
        });

        it('uses selected option label or uppercase language', () => {
            expect(
                resolveLanguageLabel({
                    language: 'sql',
                    hasHeaderLeft: false,
                    selectedLanguage: 'snowflake',
                    languageOptions: [
                        { value: 'ansi', label: 'ANSI SQL' },
                        { value: 'snowflake', label: 'Snowflake' },
                    ],
                }),
            ).toBe('Snowflake');
            expect(
                resolveLanguageLabel({
                    language: 'yaml',
                    hasHeaderLeft: false,
                }),
            ).toBe('YAML');
        });
    });

    describe('shouldShowLineNumbers', () => {
        it('enables numbers for highlights or hide-gutter mode', () => {
            expect(shouldShowLineNumbers({ showLineNumbers: false, hideLineNumbers: false })).toBe(false);
            expect(shouldShowLineNumbers({ showLineNumbers: false, hideLineNumbers: true })).toBe(true);
            expect(
                shouldShowLineNumbers({ showLineNumbers: false, hideLineNumbers: false, highlightedLines: [1] }),
            ).toBe(true);
        });
    });

    describe('getLanguageControlMode', () => {
        const options = [
            { value: 'a', label: 'A' },
            { value: 'b', label: 'B' },
            { value: 'c', label: 'C' },
        ];

        it('returns static without change handler or with one option', () => {
            expect(getLanguageControlMode(options)).toBe('static');
            expect(getLanguageControlMode([options[0]], () => undefined)).toBe('static');
        });

        it('returns tabs for exactly two changeable options', () => {
            expect(getLanguageControlMode(options.slice(0, 2), () => undefined)).toBe('tabs');
        });

        it('returns select for three or more changeable options', () => {
            expect(getLanguageControlMode(options, () => undefined)).toBe('select');
        });
    });

    describe('resolveStaticLanguageLabel', () => {
        it('prefers staticLabel then selected then first option', () => {
            const options = [
                { value: 'a', label: 'A' },
                { value: 'b', label: 'B' },
            ];
            expect(resolveStaticLanguageLabel(options, 'b', 'Static')).toBe('Static');
            expect(resolveStaticLanguageLabel(options, 'b', null)).toBe('B');
            expect(resolveStaticLanguageLabel(options, undefined, null)).toBe('A');
        });
    });

    describe('mapLanguageOptionsToTabItems', () => {
        it('maps values to tab keys and optional test ids', () => {
            expect(
                mapLanguageOptionsToTabItems(
                    [
                        { value: 'ansi', label: 'ANSI' },
                        { value: 'sf', label: 'SF' },
                    ],
                    'lang',
                ),
            ).toEqual([
                { key: 'ansi', label: 'ANSI', dataTestId: 'lang-ansi' },
                { key: 'sf', label: 'SF', dataTestId: 'lang-sf' },
            ]);
        });
    });

    describe('mergeHighlightedLineProps', () => {
        it('merges highlight styles while preserving external props', () => {
            expect(
                mergeHighlightedLineProps(2, [2], 'yellow', () => ({
                    className: 'row',
                    style: { color: 'red' },
                })),
            ).toEqual({
                className: 'row',
                style: {
                    display: 'block',
                    backgroundColor: 'yellow',
                    color: 'red',
                },
            });
            expect(mergeHighlightedLineProps(1, [2], 'yellow')).toEqual({});
        });
    });

    describe('getCodeBlockTestId', () => {
        it('prefixes when a root test id exists', () => {
            expect(getCodeBlockTestId('sql', 'copy', 'code-block-copy')).toBe('sql-copy');
            expect(getCodeBlockTestId(undefined, 'copy', 'code-block-copy')).toBe('code-block-copy');
        });
    });
});
