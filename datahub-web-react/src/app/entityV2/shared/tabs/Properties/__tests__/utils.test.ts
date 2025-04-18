import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { PropertyRow } from '../types';
import { filterStructuredProperties } from '../utils';

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
