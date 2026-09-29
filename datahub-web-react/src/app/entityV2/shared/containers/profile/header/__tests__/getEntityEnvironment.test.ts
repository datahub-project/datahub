import { getEntityEnvironment } from '@app/entityV2/shared/containers/profile/header/getEntityEnvironment';

import { EntityType, FabricType } from '@types';

describe('getEntityEnvironment', () => {
    it('returns origin for datasets', () => {
        expect(getEntityEnvironment({ type: EntityType.Dataset, origin: FabricType.Prod } as any)).toBe(
            FabricType.Prod,
        );
    });
    it('returns properties.origin for containers', () => {
        expect(
            getEntityEnvironment({ type: EntityType.Container, properties: { origin: FabricType.Dev } } as any),
        ).toBe(FabricType.Dev);
    });
    it('returns origin for ML models', () => {
        expect(getEntityEnvironment({ type: EntityType.Mlmodel, origin: FabricType.Qa } as any)).toBe(FabricType.Qa);
    });
    it('returns origin for ML model groups', () => {
        expect(getEntityEnvironment({ type: EntityType.MlmodelGroup, origin: FabricType.Test } as any)).toBe(
            FabricType.Test,
        );
    });
    it('returns null for containers without origin', () => {
        expect(getEntityEnvironment({ type: EntityType.Container, properties: {} } as any)).toBeNull();
    });
    it('returns null for entity types without environment', () => {
        expect(getEntityEnvironment({ type: EntityType.Chart } as any)).toBeNull();
    });
    it('returns null for null entity', () => {
        expect(getEntityEnvironment(null)).toBeNull();
    });
});
