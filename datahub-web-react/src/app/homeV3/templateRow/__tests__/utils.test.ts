import { describe, expect, it } from 'vitest';

import { WrappedRow } from '@app/homeV3/templateRow/types';
import { wrapRows } from '@app/homeV3/templateRow/utils';

import { DataHubPageModuleType, DataHubPageTemplateRow, EntityType, PageModuleScope } from '@types';

const MOCKED_TIMESTAMP = 1752056099724;

describe('wrapRows', () => {
    const makeRow = (moduleCount: number): DataHubPageTemplateRow => ({
        modules: Array.from({ length: moduleCount }).map((_, i) => ({
            urn: `urn:li:module:${i}`,
            type: EntityType.DatahubPageModule,
            properties: {
                name: `Module ${i}`,
                type: DataHubPageModuleType.OwnedAssets,
                visibility: {
                    scope: PageModuleScope.Global,
                },
                created: {
                    time: MOCKED_TIMESTAMP,
                },
                lastModified: {
                    time: MOCKED_TIMESTAMP,
                },
                params: {},
            },
        })),
    });

    it('should split modules into chunks of default size (3)', () => {
        const rows: DataHubPageTemplateRow[] = [
            makeRow(7), // 7 modules → [3, 3, 1]
        ];

        const result = wrapRows(rows);

        expect(result).toHaveLength(3);
        expect(result[0].modules).toHaveLength(3);
        expect(result[1].modules).toHaveLength(3);
        expect(result[2].modules).toHaveLength(1);
        expect(result[0].originRowIndex).toBe(0);
        expect(result[0].rowIndex).toBe(0);
        expect(result[1].rowIndex).toBe(1);
        expect(result[2].rowIndex).toBe(2);
    });

    it('should respect custom chunk size', () => {
        const rows: DataHubPageTemplateRow[] = [makeRow(5)];

        const result = wrapRows(rows, 2); // chunk size = 2 → [2, 2, 1]

        expect(result).toHaveLength(3);
        expect(result[0].modules).toHaveLength(2);
        expect(result[1].modules).toHaveLength(2);
        expect(result[2].modules).toHaveLength(1);
    });

    it('should handle multiple rows correctly', () => {
        const rows: DataHubPageTemplateRow[] = [
            makeRow(4), // → [3, 1]
            makeRow(5), // → [3, 2]
        ];

        const result = wrapRows(rows);

        expect(result).toHaveLength(4);

        // First row
        expect(result[0].originRowIndex).toBe(0);
        expect(result[0].rowIndex).toBe(0);

        expect(result[1].originRowIndex).toBe(0);
        expect(result[1].rowIndex).toBe(1);

        // Second row
        expect(result[2].originRowIndex).toBe(1);
        expect(result[2].rowIndex).toBe(2);

        expect(result[3].originRowIndex).toBe(1);
        expect(result[3].rowIndex).toBe(3);
    });

    it('should return empty array if no rows provided', () => {
        const result = wrapRows([]);
        expect(result).toEqual<WrappedRow[]>([]);
    });

    it('should return empty array if all rows have no modules', () => {
        const rows: DataHubPageTemplateRow[] = [{ modules: [] }, { modules: [] }];

        const result = wrapRows(rows);
        expect(result).toEqual<WrappedRow[]>([]);
    });

    it('should handle exact multiples of chunk size', () => {
        const rows: DataHubPageTemplateRow[] = [
            makeRow(6), // 6 modules → [3, 3]
        ];

        const result = wrapRows(rows);

        expect(result).toHaveLength(2);
        expect(result[0].modules).toHaveLength(3);
        expect(result[1].modules).toHaveLength(3);
    });
});
