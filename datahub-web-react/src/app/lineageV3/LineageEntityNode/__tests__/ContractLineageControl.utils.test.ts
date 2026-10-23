import { getFilterCounts } from '@app/lineageV3/LineageEntityNode/ContractLineageControl.utils';

const leaves = (n: number) => Array.from({ length: n }, (_, i) => `leaf${i}`);
const sources = (n: number) => Array.from({ length: n }, (_, i) => `source${i}`);

describe('getFilterCounts', () => {
    it('counts only paginated children, not transformations passed through to reach them', () => {
        // e.g. a dbt model with 11 dbt sources, each in front of its warehouse table
        const filter = {
            contents: leaves(11),
            allChildren: new Set([...leaves(11), ...sources(11)]),
            shown: new Set([...leaves(4), ...sources(4)]),
        };

        // The lineage search also counts the 11 dbt sources
        expect(getFilterCounts(filter, 22, false)).toEqual({ shown: 4, total: 11 });
        expect(getFilterCounts(filter, undefined, true)).toEqual({ shown: 4, total: 11 });
    });

    it('uses the lineage search total when there are no transformations', () => {
        const filter = { contents: leaves(6), allChildren: new Set(leaves(6)), shown: new Set(leaves(4)) };

        expect(getFilterCounts(filter, 8, false)).toEqual({ shown: 4, total: 8 });
        expect(getFilterCounts(filter, undefined, false)).toEqual({ shown: 4, total: 6 });
        expect(getFilterCounts(filter, 8, true)).toEqual({ shown: 4, total: 6 });
    });

    it('never shows more than the total', () => {
        const filter = { contents: leaves(6), allChildren: new Set(leaves(6)), shown: new Set(leaves(6)) };

        expect(getFilterCounts(filter, 5, false)).toEqual({ shown: 5, total: 5 });
    });
});
