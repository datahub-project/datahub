import {
    filterEnvironments,
    isValidEnvironmentId,
    normalizeEnvironmentId,
} from '@app/settingsV2/environments/environments.utils';

import { CustomFabricType, EntityType } from '@types';

const environment = (id: string, displayName?: string): CustomFabricType => ({
    urn: `urn:li:customFabricType:${id}`,
    type: EntityType.CustomFabricType,
    id,
    properties: displayName ? { displayName } : null,
});

describe('environments utils', () => {
    it('normalizes and validates ids like the server', () => {
        expect(normalizeEnvironmentId(' perf_test ')).toBe('PERF_TEST');
        expect(isValidEnvironmentId('perf_test')).toBe(true);
        expect(isValidEnvironmentId('PERF2')).toBe(true);
        expect(isValidEnvironmentId('perf-test')).toBe(false);
        expect(isValidEnvironmentId('2PERF')).toBe(false);
        expect(isValidEnvironmentId('')).toBe(false);
        expect(isValidEnvironmentId('A'.repeat(65))).toBe(false);
    });

    it('filters by id or display name, ignoring case', () => {
        const environments = [environment('PERF', 'Performance'), environment('LOAD')];

        expect(filterEnvironments(environments, 'perf').map((e) => e.id)).toEqual(['PERF']);
        expect(filterEnvironments(environments, 'formance').map((e) => e.id)).toEqual(['PERF']);
        expect(filterEnvironments(environments, '  ').map((e) => e.id)).toEqual(['PERF', 'LOAD']);
    });
});
