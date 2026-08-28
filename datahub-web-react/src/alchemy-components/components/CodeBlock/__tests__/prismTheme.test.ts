import { describe, expect, it } from 'vitest';

import { getCodeBlockPrismStyle } from '@components/components/CodeBlock/prismTheme';

import dark from '@conf/theme/colorThemes/dark';
import light from '@conf/theme/colorThemes/light';

describe('getCodeBlockPrismStyle', () => {
    it('should map Prism tokens onto light semantic colors', () => {
        const style = getCodeBlockPrismStyle(light);

        expect(style.keyword).toEqual({ color: light.textBrand });
        expect(style.string).toEqual({ color: light.textSuccess });
        expect(style.comment).toEqual({ color: light.textTertiary, fontStyle: 'italic' });
        expect(style.number).toEqual({ color: light.textInformation });
        expect(style.deleted).toEqual({ color: light.textError });
        expect(style.function).toEqual({ color: light.hyperlinks });
        expect(style['code[class*="language-"]']).toMatchObject({
            color: light.text,
            background: 'transparent',
        });
    });

    it('should follow dark semantic tokens without checking theme id', () => {
        const style = getCodeBlockPrismStyle(dark);

        expect(style.keyword).toEqual({ color: dark.textBrand });
        expect(style.string).toEqual({ color: dark.textSuccess });
        expect(style.comment).toEqual({ color: dark.textTertiary, fontStyle: 'italic' });
        expect(style['pre[class*="language-"]::selection']).toEqual({ background: dark.bgSelectedSubtle });
    });
});
