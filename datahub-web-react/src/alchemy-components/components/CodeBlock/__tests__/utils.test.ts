import { describe, expect, it } from 'vitest';

import { CODE_BLOCK_EDITOR_DEFAULT_MAX_HEIGHT } from '@components/components/CodeBlock/defaults';
import {
    getCodeBlockTestId,
    getLanguageControlMode,
    insertTextAtSelection,
    isCodeBlockEditable,
    isCodeBlockHeightCapped,
    isCodeFormatShortcut,
    isLineHighlighted,
    mapLanguageOptionsToTabItems,
    mergeHighlightedLineProps,
    resolveCodeBlockBodyMaxHeight,
    resolveCodeBlockStatusDisplay,
    resolveLanguageLabel,
    resolveStaticLanguageLabel,
    shouldShowCodeBlockHeader,
    shouldShowFormatButton,
    shouldShowLineNumbers,
    shouldShowValidateButton,
    syncCodeBlockHighlightScroll,
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
            expect(
                shouldShowCodeBlockHeader({
                    showHeader: false,
                    showCopy: false,
                    showFormat: true,
                }),
            ).toBe(true);
        });
    });

    describe('isCodeBlockEditable', () => {
        it('should enable the editor when onChange is provided', () => {
            expect(isCodeBlockEditable({ onChange: () => undefined })).toBe(true);
            expect(isCodeBlockEditable({ onChange: () => undefined, isReadOnly: true })).toBe(false);
            expect(isCodeBlockEditable({})).toBe(false);
        });
    });

    describe('shouldShowFormatButton', () => {
        it('should show format only while editable unless overridden', () => {
            expect(shouldShowFormatButton({ isEditable: true })).toBe(true);
            expect(shouldShowFormatButton({ isEditable: true, showFormat: false })).toBe(false);
            expect(shouldShowFormatButton({ isEditable: false, showFormat: true })).toBe(false);
        });
    });

    describe('shouldShowValidateButton', () => {
        it('should only show when validateSyntax is enabled on an editable editor', () => {
            expect(shouldShowValidateButton({ isEditable: true, validateSyntax: true })).toBe(true);
            expect(shouldShowValidateButton({ isEditable: true, validateSyntax: false })).toBe(false);
            expect(shouldShowValidateButton({ isEditable: false, validateSyntax: true })).toBe(false);
        });
    });

    describe('isCodeFormatShortcut', () => {
        it('should match Shift+Alt+F without meta or ctrl', () => {
            expect(
                isCodeFormatShortcut({ key: 'f', shiftKey: true, altKey: true, metaKey: false, ctrlKey: false }),
            ).toBe(true);
            expect(
                isCodeFormatShortcut({ key: 'f', shiftKey: true, altKey: true, metaKey: true, ctrlKey: false }),
            ).toBe(false);
        });
    });

    describe('insertTextAtSelection', () => {
        it('should insert at the caret and advance it', () => {
            expect(insertTextAtSelection('ab', 1, 1, '  ')).toEqual({ value: 'a  b', caret: 3 });
            expect(insertTextAtSelection('abcd', 1, 3, '  ')).toEqual({ value: 'a  d', caret: 3 });
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

    describe('resolveCodeBlockStatusDisplay', () => {
        const msgs = {
            syntaxErrorMessage: 'syntax error',
            syntaxWarningMessage: 'sql warning',
        };

        it('should prefer parent error over syntax findings', () => {
            expect(
                resolveCodeBlockStatusDisplay({
                    error: 'Required',
                    syntaxSeverity: 'warning',
                    ...msgs,
                }),
            ).toEqual({
                displayError: 'Required',
                displayWarning: '',
                statusDetails: [],
                isInvalid: true,
                hasWarning: false,
            });
        });

        it('should surface syntax warnings as soft chrome', () => {
            expect(
                resolveCodeBlockStatusDisplay({
                    syntaxSeverity: 'warning',
                    syntaxDetails: ['near line 1, column 13 — unexpected "o"'],
                    ...msgs,
                }),
            ).toEqual({
                displayError: '',
                displayWarning: 'sql warning near line 1, column 13 — unexpected "o".',
                statusDetails: [],
                isInvalid: false,
                hasWarning: true,
            });
        });

        it('should list multiple syntax details under the base warning', () => {
            expect(
                resolveCodeBlockStatusDisplay({
                    syntaxSeverity: 'warning',
                    syntaxDetails: ['SELECT is missing columns', 'FROM is missing a table name'],
                    ...msgs,
                }),
            ).toEqual({
                displayError: '',
                displayWarning: 'sql warning',
                statusDetails: ['SELECT is missing columns', 'FROM is missing a table name'],
                isInvalid: false,
                hasWarning: true,
            });
        });

        it('should surface syntax errors as hard chrome', () => {
            expect(
                resolveCodeBlockStatusDisplay({
                    syntaxSeverity: 'error',
                    syntaxDetails: ['Expected property name'],
                    ...msgs,
                }),
            ).toEqual({
                displayError: 'syntax error Expected property name.',
                displayWarning: '',
                statusDetails: [],
                isInvalid: true,
                hasWarning: false,
            });
        });
    });

    describe('resolveCodeBlockBodyMaxHeight', () => {
        it('should cap writable editors at 400px by default', () => {
            expect(resolveCodeBlockBodyMaxHeight({ isEditable: true })).toBe(CODE_BLOCK_EDITOR_DEFAULT_MAX_HEIGHT);
            expect(resolveCodeBlockBodyMaxHeight({ isEditable: false })).toBeUndefined();
        });

        it('should honor an explicit max height, including none', () => {
            expect(resolveCodeBlockBodyMaxHeight({ isEditable: true, maxHeight: 240 })).toBe(240);
            expect(resolveCodeBlockBodyMaxHeight({ isEditable: false, maxHeight: '50vh' })).toBe('50vh');
            expect(resolveCodeBlockBodyMaxHeight({ isEditable: true, maxHeight: 'none' })).toBe('none');
        });
    });

    describe('isCodeBlockHeightCapped', () => {
        it('should treat none and missing as uncapped', () => {
            expect(isCodeBlockHeightCapped(undefined)).toBe(false);
            expect(isCodeBlockHeightCapped('none')).toBe(false);
            expect(isCodeBlockHeightCapped(400)).toBe(true);
            expect(isCodeBlockHeightCapped('50vh')).toBe(true);
        });
    });

    describe('syncCodeBlockHighlightScroll', () => {
        it('should copy scroll offsets onto the overlay', () => {
            const textarea = { scrollTop: 40, scrollLeft: 12 } as HTMLTextAreaElement;
            const highlight = { scrollTop: 0, scrollLeft: 0 } as HTMLElement;
            syncCodeBlockHighlightScroll(textarea, highlight);
            expect(highlight.scrollTop).toBe(40);
            expect(highlight.scrollLeft).toBe(12);
        });

        it('should no-op when a node is missing', () => {
            expect(() => syncCodeBlockHighlightScroll(null, null)).not.toThrow();
        });
    });
});
