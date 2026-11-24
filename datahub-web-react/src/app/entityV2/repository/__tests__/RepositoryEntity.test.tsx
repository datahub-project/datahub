import { describe, expect, it } from 'vitest';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import { RepositoryEntity } from '@app/entityV2/repository/RepositoryEntity';

const platform = { urn: 'urn:li:dataPlatform:github', name: 'github' };
const aliasPlatform = { urn: 'urn:li:dataPlatform:aliased', name: 'aliased' };

// `repositoryPlatform` is a query-side alias that exists only on the per-operation result
// shape, never on the generated entity type, so these fixtures are cast.
type OverrideInput = Parameters<RepositoryEntity['getOverridePropertiesFromEntity']>[0];
const asEntity = (data: Record<string, unknown>) => data as unknown as OverrideInput;

describe('RepositoryEntity capabilities', () => {
    it('supports lineage', () => {
        expect(new RepositoryEntity().supportedCapabilities().has(EntityCapabilityType.LINEAGE)).toBe(true);
    });
});

describe('RepositoryEntity platform alias resolution', () => {
    const entity = new RepositoryEntity();

    it('reads the plain platform field from the profile-page shape', () => {
        expect(entity.getOverridePropertiesFromEntity(asEntity({ platform })).platform).toBe(platform);
    });

    it('falls back to the repositoryPlatform alias from the search shape', () => {
        expect(entity.getOverridePropertiesFromEntity(asEntity({ repositoryPlatform: aliasPlatform })).platform).toBe(
            aliasPlatform,
        );
    });

    it('prefers the plain field when a query supplies both shapes', () => {
        expect(
            entity.getOverridePropertiesFromEntity(asEntity({ platform, repositoryPlatform: aliasPlatform })).platform,
        ).toBe(platform);
    });

    it('leaves platform undefined when neither shape is present', () => {
        expect(entity.getOverridePropertiesFromEntity(asEntity({})).platform).toBeUndefined();
    });
});
