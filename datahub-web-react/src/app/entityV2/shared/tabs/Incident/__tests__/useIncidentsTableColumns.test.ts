import { describe, expect, it, vi } from 'vitest';

import { useIncidentsTableColumns } from '@app/entityV2/shared/tabs/Incident/hooks';
import { renderHook } from '@utils/test-utils/renderHook';

// The table sorts ascending on the first header click; these columns put the most relevant
// incidents first on that click (newest, most assets, most assignees, category A to Z).
const firstClickOrder = (columnKey: string, rows: any[]) => {
    const { result } = renderHook(() => useIncidentsTableColumns(vi.fn()));
    const sorter = result.current.find((column) => column.key === columnKey)?.sorter as (a: any, b: any) => number;
    return rows.slice().sort(sorter);
};

describe('useIncidentsTableColumns sorters', () => {
    it('sorts Opened newest first', () => {
        const sorted = firstClickOrder('created', [{ created: 1 }, { created: 3 }, { created: 2 }]);
        expect(sorted.map((row) => row.created)).toEqual([3, 2, 1]);
    });

    it('sorts Category alphabetically', () => {
        const sorted = firstClickOrder('type', [{ type: 'OPERATIONAL' }, { type: 'FRESHNESS' }]);
        expect(sorted.map((row) => row.type)).toEqual(['FRESHNESS', 'OPERATIONAL']);
    });

    it('sorts Assets and Assignees by most first', () => {
        const byAssets = firstClickOrder('linkedAssets', [{ linkedAssets: [1] }, { linkedAssets: [1, 2, 3] }]);
        expect(byAssets.map((row) => row.linkedAssets.length)).toEqual([3, 1]);

        const byAssignees = firstClickOrder('assignees', [{ assignees: [] }, { assignees: [1, 2] }]);
        expect(byAssignees.map((row) => row.assignees.length)).toEqual([2, 0]);
    });
});
