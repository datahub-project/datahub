import { getEntityEnvironment } from '@app/entityV2/shared/containers/profile/header/getEntityEnvironment';

import { EntityType, FabricType } from '@types';

describe('getEntityEnvironment', () => {
    it('returns origin for datasets', () => {
        expect(getEntityEnvironment({ type: EntityType.Dataset, origin: FabricType.Prod } as any)).toBe(
            FabricType.Prod,
        );
    });
    it('returns properties.env for containers', () => {
        expect(getEntityEnvironment({ type: EntityType.Container, properties: { env: FabricType.Dev } } as any)).toBe(
            FabricType.Dev,
        );
    });
    it('returns null for containers without env', () => {
        expect(getEntityEnvironment({ type: EntityType.Container, properties: {} } as any)).toBeNull();
    });
    it('returns null for entity types without environment', () => {
        expect(getEntityEnvironment({ type: EntityType.Chart } as any)).toBeNull();
    });
    it('returns null for null entity', () => {
        expect(getEntityEnvironment(null)).toBeNull();
    });
});
