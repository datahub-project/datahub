import { dataJob1, dataset1, dataset2 } from '../../../../Mocks';
import { FetchedEntity } from '../../types';
import { FineGrainedLineage } from '../../../../types.generated';
import { extendColumnLineage } from '../extendAsyncEntities';

describe('extendColumnLineage', () => {
    it('should update fineGrainedMap to draw lines from downstream and upstream datasets with a datajob in between', () => {
        const dataJobWithCLL = {
            ...dataJob1,
            name: '',
            fineGrainedLineages: [
                {
                    upstreams: [{ urn: dataset1.urn, path: 'test1' }],
                    downstreams: [{ urn: dataset2.urn, path: 'test2' }],
                },
                {
                    upstreams: [{ urn: dataset1.urn, path: 'test3' }],
                    downstreams: [{ urn: dataset2.urn, path: 'test4' }],
                },
            ] as FineGrainedLineage[],
        };
        const fetchedEntities = new Map([[dataJobWithCLL.urn, dataJobWithCLL as FetchedEntity]]);
        const fineGrainedMap = { forward: {}, reverse: {} };
        extendColumnLineage(dataJobWithCLL, fineGrainedMap, {}, fetchedEntities);

        expect(fineGrainedMap).toMatchObject({
            forward: {
                [dataJob1.urn]: {
                    test1: { [dataset2.urn]: ['test2'] },
                    test3: { [dataset2.urn]: ['test4'] },
                },
                [dataset1.urn]: {
                    test1: { [dataJob1.urn]: ['test1'] },
                    test3: { [dataJob1.urn]: ['test3'] },
                },
            },
            reverse: {
                [dataJob1.urn]: { test1: { [dataset1.urn]: ['test1'] }, test3: { [dataset1.urn]: ['test3'] } },
                [dataset2.urn]: { test4: { [dataJob1.urn]: ['test3'] }, test2: { [dataJob1.urn]: ['test1'] } },
            },
        });
    });
});
