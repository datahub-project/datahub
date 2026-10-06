import { Linter } from 'eslint';
import { describe, expect, it } from 'vitest';

import tsParser from '@typescript-eslint/parser';

// The rule is a CommonJS module (loaded by ESLint via eslint-plugin-rulesdir).
import rule from '../no-hardcoded-colors.js';

function lint(code: string): string[] {
    const linter = new Linter();
    linter.defineParser('ts', tsParser as never);
    linter.defineRule('t/no-hardcoded-colors', rule as never);
    return linter
        .verify(code, {
            parser: 'ts',
            parserOptions: { ecmaVersion: 2020, sourceType: 'module', ecmaFeatures: { jsx: true } },
            rules: { 't/no-hardcoded-colors': 'error' },
        })
        .map((m) => m.message);
}

// Render a theme.styles[<key>] read inside a styled-component template.
const stylesRead = (key: string) => `const C = styled.div\`x: \${(p) => p.theme.styles['${key}']};\`;`;

describe('no-hardcoded-colors: theme.styles surface', () => {
    // The whole color-bearing surface is banned, not just the four legacy brand keys.
    it.each([
        'layout-header-color',
        'body-background',
        'border-color-base',
        'homepage-background-upper-fade',
        'homepage-background-lower-fade',
        'homepage-text-color',
        'box-shadow',
        'box-shadow-hover',
        'box-shadow-navbar-redesign',
        'highlight-border-color',
        'divider-color',
        'disabled-color',
        'text-color',
        // original brand keys still banned
        'primary-color',
        'primary-color-dark',
        'primary-color-light',
        'highlight-color',
    ])('bans theme.styles[%s]', (key) => {
        const msgs = lint(stylesRead(key));
        expect(msgs).toHaveLength(1);
        expect(msgs[0]).toMatch(/theme\.styles/);
    });

    // border-radius-* are radii, not colors — they have no theme.colors.* equivalent.
    it.each(['border-radius-navbar-redesign', 'border-radius-base'])(
        'does NOT ban non-color structural key theme.styles[%s]',
        (key) => {
            expect(lint(stylesRead(key))).toHaveLength(0);
        },
    );

    it('still flags raw hex and rgb', () => {
        expect(lint('const C = styled.div`color: #ffffff;`;')).toHaveLength(1);
        expect(lint('const C = styled.div`color: rgb(1,2,3);`;')).toHaveLength(1);
    });
});
