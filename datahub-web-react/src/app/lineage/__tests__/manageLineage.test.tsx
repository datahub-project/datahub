import { dataFlow1, dataJob1, dataset1, dataset2, dataset3 } from '../../../Mocks';
import { existsInEntitiesToAdd } from '../manage/AddEntityEdge';
import { buildUpdateLineagePayload } from '../utils/manageLineageUtils';
import { Direction } from '../types';

describe('existsInEntitiesToAdd', () => {
    it('should return false if the search result is not in entitiesAlreadyAdded', () => {
        const result = { entity: { urn: 'urn:li:test' } } as any;
        const entitiesAlreadyAdded = [{ urn: 'urn:li:testing123' }] as any;
        const exists = existsInEntitiesToAdd(result, entitiesAlreadyAdded);

        expect(exists).toBe(false);
    });

    it('should return true if the search result is in entitiesAlreadyAdded', () => {
        const result = { entity: { urn: 'urn:li:test' } } as any;
        const entitiesAlreadyAdded = [{ urn: 'urn:li:testing123' }, { urn: 'urn:li:test' }] as any;
        const exists = existsInEntitiesToAdd(result, entitiesAlreadyAdded);

        expect(exists).toBe(true);
    });
});

describe('buildUpdateLineagePayload', () => {
    it('should build update lineage payload properly in the upstream direction', () => {
        const entitiesToAdd = [dataFlow1, dataset2];
        const entitiesToRemove = [dataJob1, dataset3];
        const entityUrn = dataset1.urn;
        const payload = buildUpdateLineagePayload(Direction.Upstream, entitiesToAdd, entitiesToRemove, entityUrn);

        expect(payload).toMatchObject({
            edgesToAdd: [
                { upstreamUrn: dataFlow1.urn, downstreamUrn: entityUrn },
                { upstreamUrn: dataset2.urn, downstreamUrn: entityUrn },
            ],
            edgesToRemove: [
                { upstreamUrn: dataJob1.urn, downstreamUrn: entityUrn },
                { upstreamUrn: dataset3.urn, downstreamUrn: entityUrn },
            ],
        });
    });

    it('should build update lineage payload properly in the downstream direction', () => {
        const entitiesToAdd = [dataFlow1, dataset2];
        const entitiesToRemove = [dataJob1, dataset3];
        const entityUrn = dataset1.urn;
        const payload = buildUpdateLineagePayload(Direction.Downstream, entitiesToAdd, entitiesToRemove, entityUrn);

        expect(payload).toMatchObject({
            edgesToAdd: [
                { upstreamUrn: entityUrn, downstreamUrn: dataFlow1.urn },
                { upstreamUrn: entityUrn, downstreamUrn: dataset2.urn },
            ],
            edgesToRemove: [
                { upstreamUrn: entityUrn, downstreamUrn: dataJob1.urn },
                { upstreamUrn: entityUrn, downstreamUrn: dataset3.urn },
            ],
        });
    });
});
