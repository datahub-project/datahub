import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- server tooling has no src alias
import { applyFlagOverrides, parseFlagOverrides } from '../flagOverrides';

describe('local flag overrides', () => {
    it('parses booleans and strings and overlays appConfig', () => {
        const flags = parseFlagOverrides('showBrowseV2=true,theme=dark,showNavBarRedesign=false');
        const body = { data: { appConfig: { featureFlags: { showBrowseV2: false } } } };
        applyFlagOverrides(body, flags);
        expect(body.data.appConfig.featureFlags).toEqual({
            showBrowseV2: true,
            theme: 'dark',
            showNavBarRedesign: false,
        });
        expect(applyFlagOverrides({ data: { me: {} } }, flags)).toEqual({ data: { me: {} } });
    });
});
