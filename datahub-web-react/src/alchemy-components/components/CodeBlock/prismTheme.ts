import type { CSSProperties } from 'react';

import ColorTheme from '@conf/theme/colorThemes/types';

export type PrismStyle = { [key: string]: CSSProperties };

function token(color: string, extra?: CSSProperties): CSSProperties {
    return { color, ...extra };
}

/**
 * Builds Prism token colors from semantic theme tokens so light, dark, and
 * branded themes (FIS) stay in sync without checking `theme.id`.
 *
 * @param colors - Semantic color tokens from the current theme
 * @returns Prism style map for `react-syntax-highlighter`
 */
export function getCodeBlockPrismStyle(colors: ColorTheme): PrismStyle {
    const muted = token(colors.textTertiary, { fontStyle: 'italic' });
    const brand = token(colors.textBrand);
    const info = token(colors.textInformation);
    const success = token(colors.textSuccess);
    const error = token(colors.textError);
    const link = token(colors.hyperlinks);
    const body = token(colors.text);

    return {
        'code[class*="language-"]': {
            color: colors.text,
            background: 'transparent',
        },
        'pre[class*="language-"]': {
            color: colors.text,
            background: 'transparent',
        },
        'pre[class*="language-"]::selection': { background: colors.bgSelectedSubtle },
        'pre[class*="language-"] ::selection': { background: colors.bgSelectedSubtle },
        'code[class*="language-"]::selection': { background: colors.bgSelectedSubtle },
        'code[class*="language-"] ::selection': { background: colors.bgSelectedSubtle },
        comment: muted,
        prolog: muted,
        doctype: muted,
        cdata: muted,
        punctuation: body,
        operator: body,
        string: success,
        'attr-value': success,
        inserted: success,
        number: info,
        boolean: info,
        constant: info,
        property: info,
        variable: info,
        symbol: info,
        entity: info,
        regex: info,
        keyword: brand,
        atrule: brand,
        'attr-name': brand,
        function: link,
        url: link,
        tag: link,
        selector: link,
        deleted: error,
        important: { fontWeight: 'bold' },
        bold: { fontWeight: 'bold' },
        italic: { fontStyle: 'italic' },
    };
}
