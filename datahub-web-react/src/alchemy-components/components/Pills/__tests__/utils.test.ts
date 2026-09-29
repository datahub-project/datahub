import { describe, expect, it } from 'vitest';

import { getPillStyle } from '@components/components/Pills/utils';

import themeV2 from '@conf/theme/themeV2';
import themeV2Dark from '@conf/theme/themeV2Dark';

describe.each([themeV2, themeV2Dark])('Pill styles for $id', (theme) => {
    it('uses the semantic border token for a gray outline pill', () => {
        const style = getPillStyle({
            variant: 'outline',
            color: 'gray',
            size: 'md',
            clickable: true,
            theme,
        });

        expect(style.border).toBe(`1px solid ${theme.colors.border}`);
        expect(style.backgroundColor).toBe('transparent');
    });

    it('uses the semantic brand border for a primary outline pill', () => {
        const style = getPillStyle({
            variant: 'outline',
            color: 'primary',
            size: 'md',
            clickable: true,
            theme,
        });

        expect(style.border).toBe(`1px solid ${theme.colors.borderBrand}`);
    });
});
