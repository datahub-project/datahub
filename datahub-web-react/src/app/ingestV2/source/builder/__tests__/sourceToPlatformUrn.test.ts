import { CUSTOM, SOURCE_TO_PLATFORM_URN } from '@app/ingestV2/source/builder/constants';
import sources from '@app/ingestV2/source/builder/sources.json';

// useGetSourceLogoUrl only has the tile name, so it must resolve to the tile's platform URN.
// LookML intentionally shows the Looker logo.
const INTENTIONAL_LOGO_OVERRIDES = new Set(['lookml']);

describe('sources.json', () => {
    // The source picker keys its cards by name; several tiles share a platform URN.
    it('has unique tile names', () => {
        const names = sources.map((source) => source.name);
        expect(names.filter((name, index) => names.indexOf(name) !== index)).toEqual([]);
    });
});

describe('SOURCE_TO_PLATFORM_URN', () => {
    it('resolves every tile name to the platform URN of its tile', () => {
        const mismatches = sources
            .filter((source) => source.name !== CUSTOM && !INTENTIONAL_LOGO_OVERRIDES.has(source.name))
            .filter(
                (source) =>
                    (SOURCE_TO_PLATFORM_URN[source.name] ?? `urn:li:dataPlatform:${source.name}`) !== source.urn,
            )
            .map((source) => `${source.name} -> ${source.urn}`);

        expect(mismatches).toEqual([]);
    });
});
