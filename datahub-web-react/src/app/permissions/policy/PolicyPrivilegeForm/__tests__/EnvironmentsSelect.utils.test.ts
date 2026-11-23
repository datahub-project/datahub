import { getEnvironmentOptions } from '@app/permissions/policy/PolicyPrivilegeForm/EnvironmentsSelect.utils';

import { CustomFabricType, EntityType } from '@types';

describe('getEnvironmentOptions', () => {
    const perf: CustomFabricType = {
        urn: 'urn:li:customFabricType:PERF',
        type: EntityType.CustomFabricType,
        id: 'PERF',
        properties: { displayName: 'Performance' },
    };

    it('lists built-in environments without OTHER, then custom ones by display name', () => {
        const options = getEnvironmentOptions([perf], []);

        expect(options).toContainEqual({ value: 'PROD', label: 'PROD' });
        expect(options.map((option) => option.value)).not.toContain('OTHER');
        expect(options[options.length - 1]).toEqual({ value: 'PERF', label: 'Performance' });
    });

    it('keeps a selected value that is no longer registered, so it can be removed', () => {
        const options = getEnvironmentOptions([], ['GONE', 'PROD']);

        expect(options.filter((option) => option.value === 'GONE')).toEqual([{ value: 'GONE', label: 'GONE' }]);
        expect(options.filter((option) => option.value === 'PROD')).toHaveLength(1);
    });
});
