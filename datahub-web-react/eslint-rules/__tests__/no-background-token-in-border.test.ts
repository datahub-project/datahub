import { Linter } from 'eslint';
import { describe, expect, it } from 'vitest';

import tsParser from '@typescript-eslint/parser';

import rule from '../no-background-token-in-border.js';

function lint(code: string): string[] {
    const linter = new Linter();
    linter.defineParser('ts', tsParser as never);
    linter.defineRule('t/no-background-token-in-border', rule as never);
    return linter
        .verify(code, {
            parser: 'ts',
            parserOptions: { ecmaVersion: 2020, sourceType: 'module', ecmaFeatures: { jsx: true } },
            rules: { 't/no-background-token-in-border': 'error' },
        })
        .map((message) => message.message);
}

const interpolation = (expression: string) => `${String.fromCharCode(36)}{${expression}}`;
const styledDeclaration = (property: string, value: string) => `const C = styled.div\`${property}: ${value};\`;`;

describe('no-background-token-in-border', () => {
    it.each([
        styledDeclaration('border', `1px solid ${interpolation('(p) => p.theme.colors.bgSurface')}`),
        styledDeclaration('border-color', interpolation('(p) => p.theme.colors.bgHover')),
        styledDeclaration('border-top', `1px solid ${interpolation('theme.colors.bg')}`),
        'const styles = { borderColor: theme.colors.bgSurfaceDarker };',
        'const styles = { outlineColor: theme.colors.bgElevated };',
        'const C = <Box borderColor={theme.colors.bgSurfaceError} />;',
    ])('rejects a background token in a border declaration', (code) => {
        expect(lint(code)).toHaveLength(1);
    });

    it.each([
        styledDeclaration('border', `1px solid ${interpolation('(p) => p.theme.colors.border')}`),
        styledDeclaration('background', interpolation('(p) => p.theme.colors.bgSurface')),
        'const styles = { borderColor: theme.colors.borderInput };',
    ])('allows correctly classified semantic tokens', (code) => {
        expect(lint(code)).toHaveLength(0);
    });
});
