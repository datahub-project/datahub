import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import { filterHiddenProperties, filterStructuredProperties } from '@app/entityV2/shared/tabs/Properties/utils';
import globalEntityRegistryV2 from '@app/globalEntityRegistryV2';

describe('filterSchemaRows', () => {
    const rows = [
        {
            displayName: 'Has PII',
            qualifiedName: 'io.acryl.ads.data_protection.has_pii',
            values: [{ value: 'yes', entity: null }],
        },
        {
            displayName: 'Discovery Date Utc',
            qualifiedName: 'io.acryl.ads.change_management.discovery_date_utc',
            values: [{ value: '2023-10-31', entity: null }],
        },
        {
            displayName: 'Link Data Location',
            qualifiedName: 'io.acryl.ads.context.data_location',
            values: [{ value: 'New York City', entity: null }],
        },
        {
            displayName: 'Number Prop',
            qualifiedName: 'io.acryl.ads.number',
            values: [{ value: 100, entity: null }],
        },
    ] as PropertyRow[];

    it('should properly filter structured properties based on field name', () => {
        const filterText = 'has pi';
        const { filteredRows, expandedRowsFromFilter } = filterStructuredProperties(
            globalEntityRegistryV2,
            rows,
            filterText,
        );

        expect(filteredRows).toMatchObject([
            {
                displayName: 'Has PII',
                qualifiedName: 'io.acryl.ads.data_protection.has_pii',
                values: [{ value: 'yes', entity: null }],
            },
        ]);
        expect(expandedRowsFromFilter).toMatchObject(
            new Set(['io', 'io.acryl', 'io.acryl.ads', 'io.acryl.ads.data_protection']),
        );
    });

    it('should properly filter structured properties based on field value', () => {
        const filterText = 'new york';
        const { filteredRows, expandedRowsFromFilter } = filterStructuredProperties(
            globalEntityRegistryV2,
            rows,
            filterText,
        );

        expect(filteredRows).toMatchObject([
            {
                displayName: 'Link Data Location',
                qualifiedName: 'io.acryl.ads.context.data_location',
                values: [{ value: 'New York City', entity: null }],
            },
        ]);
        expect(expandedRowsFromFilter).toMatchObject(
            new Set(['io', 'io.acryl', 'io.acryl.ads', 'io.acryl.ads.context']),
        );
    });

    it('should properly filter structured properties based on field value even for numbers', () => {
        const filterText = '100';
        const { filteredRows, expandedRowsFromFilter } = filterStructuredProperties(
            globalEntityRegistryV2,
            rows,
            filterText,
        );

        expect(filteredRows).toMatchObject([
            {
                displayName: 'Number Prop',
                qualifiedName: 'io.acryl.ads.number',
                values: [{ value: 100, entity: null }],
            },
        ]);
        expect(expandedRowsFromFilter).toMatchObject(new Set(['io', 'io.acryl', 'io.acryl.ads']));
    });
});

describe('filterHiddenProperties', () => {
    it('should filter out hidden properties at top level', () => {
        const rows = [
            {
                displayName: 'Visible Property',
                qualifiedName: 'io.visible',
                structuredProperty: {
                    definition: { qualifiedName: 'io.visible' },
                    settings: { isHidden: false },
                } as any,
            },
            {
                displayName: 'Hidden Property',
                qualifiedName: 'io.hidden',
                structuredProperty: {
                    definition: { qualifiedName: 'io.hidden' },
                    settings: { isHidden: true },
                } as any,
            },
        ] as PropertyRow[];

        const filtered = filterHiddenProperties(rows);
        expect(filtered).toHaveLength(1);
        expect(filtered[0].qualifiedName).toBe('io.visible');
    });

    it('should filter out hidden nested properties', () => {
        const rows = [
            {
                displayName: 'Parent',
                qualifiedName: 'io.parent',
                isParentRow: true,
                childrenCount: 2,
                children: [
                    {
                        displayName: 'Visible Child',
                        qualifiedName: 'io.parent.child1',
                        structuredProperty: {
                            definition: { qualifiedName: 'io.parent.child1' },
                            settings: { isHidden: false },
                        } as any,
                    },
                    {
                        displayName: 'Hidden Child',
                        qualifiedName: 'io.parent.child2',
                        structuredProperty: {
                            definition: { qualifiedName: 'io.parent.child2' },
                            settings: { isHidden: true },
                        } as any,
                    },
                ] as PropertyRow[],
            },
        ] as PropertyRow[];

        const filtered = filterHiddenProperties(rows);
        expect(filtered).toHaveLength(1);
        expect(filtered[0].children).toHaveLength(1);
        expect(filtered[0].childrenCount).toBe(1);
        expect(filtered[0].children?.[0].qualifiedName).toBe('io.parent.child1');
    });

    it('should handle deeply nested hidden properties', () => {
        const rows = [
            {
                displayName: 'Root',
                qualifiedName: 'io.root',
                isParentRow: true,
                childrenCount: 1,
                children: [
                    {
                        displayName: 'Level1',
                        qualifiedName: 'io.root.level1',
                        isParentRow: true,
                        childrenCount: 2,
                        structuredProperty: {
                            definition: { qualifiedName: 'io.root.level1' },
                            settings: { isHidden: false },
                        } as any,
                        children: [
                            {
                                displayName: 'Level2 Visible',
                                qualifiedName: 'io.root.level1.level2',
                                structuredProperty: {
                                    definition: { qualifiedName: 'io.root.level1.level2' },
                                    settings: { isHidden: false },
                                } as any,
                            },
                            {
                                displayName: 'Level2 Hidden',
                                qualifiedName: 'io.root.level1.level2hidden',
                                structuredProperty: {
                                    definition: { qualifiedName: 'io.root.level1.level2hidden' },
                                    settings: { isHidden: true },
                                } as any,
                            },
                        ] as PropertyRow[],
                    },
                ] as PropertyRow[],
            },
        ] as PropertyRow[];

        const filtered = filterHiddenProperties(rows);
        expect(filtered).toHaveLength(1);
        expect(filtered[0].children).toHaveLength(1);
        expect(filtered[0].children?.[0].children).toHaveLength(1);
        expect(filtered[0].children?.[0].childrenCount).toBe(1);
        expect(filtered[0].children?.[0].children?.[0].qualifiedName).toBe('io.root.level1.level2');
    });

    it('should preserve parent rows when children exist', () => {
        const rows = [
            {
                displayName: 'Parent',
                qualifiedName: 'io.parent',
                isParentRow: true,
                structuredProperty: {
                    definition: { qualifiedName: 'io.parent' },
                    settings: { isHidden: false },
                } as any,
                children: [
                    {
                        displayName: 'Child',
                        qualifiedName: 'io.parent.child',
                        structuredProperty: {
                            definition: { qualifiedName: 'io.parent.child' },
                            settings: { isHidden: false },
                        } as any,
                    },
                ] as PropertyRow[],
            },
        ] as PropertyRow[];

        const filtered = filterHiddenProperties(rows);
        expect(filtered).toHaveLength(1);
        expect(filtered[0].children).toHaveLength(1);
    });
});
