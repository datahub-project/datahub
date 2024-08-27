import { dataFlow1, dataJob1, dataset1, dataset2, dataset3 } from '../../../Mocks';
import { existsInEntitiesToAdd } from '../manage/AddEntityEdge';
import { buildUpdateLineagePayload, getValidEntityTypes } from '../utils/manageLineageUtils';
import { Direction } from '../types';
import { EntityType } from '../../../types.generated';

describe('existsInEntitiesToAdd', () => {
    it('should return false if the search result is not in entitiesAlreadyAdded', () => {
        const result = { entity: { urn: 'urn:li:test' } } as any;
        const entitiesAlreadyAdded = [{ urn: 'urn:li:testing123' }] as any;
        const exists = existsInEntitiesToAdd(result.entity, entitiesAlreadyAdded);

        expect(exists).toBe(false);
    });

    it('should return true if the search result is in entitiesAlreadyAdded', () => {
        const result = { entity: { urn: 'urn:li:test' } } as any;
        const entitiesAlreadyAdded = [{ urn: 'urn:li:testing123' }, { urn: 'urn:li:test' }] as any;
        const exists = existsInEntitiesToAdd(result.entity, entitiesAlreadyAdded);

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

describe('getValidEntityTypes', () => {
    it('should get valid entity types to query upstream lineage for datasets', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Upstream, EntityType.Dataset);
        expect(validEntityTypes).toMatchObject([EntityType.Dataset, EntityType.DataJob]);
    });
    it('should get valid entity types to query upstream lineage for charts', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Upstream, EntityType.Chart);
        expect(validEntityTypes).toMatchObject([EntityType.Dataset]);
    });
    it('should get valid entity types to query upstream lineage for dashboards', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Upstream, EntityType.Dashboard);
        expect(validEntityTypes).toMatchObject([EntityType.Chart, EntityType.Dataset]);
    });
    it('should get valid entity types to query upstream lineage for datajobs', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Upstream, EntityType.DataJob);
        expect(validEntityTypes).toMatchObject([EntityType.DataJob, EntityType.Dataset]);
    });
    it('should return an empty list if the entity type is unexpected for upstream', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Upstream, EntityType.Container);
        expect(validEntityTypes).toMatchObject([]);
    });

    it('should get valid entity types to query downstream lineage for datasets', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Downstream, EntityType.Dataset);
        expect(validEntityTypes).toMatchObject([
            EntityType.Dataset,
            EntityType.Chart,
            EntityType.Dashboard,
            EntityType.DataJob,
        ]);
    });
    it('should get valid entity types to query downstream lineage for charts', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Downstream, EntityType.Chart);
        expect(validEntityTypes).toMatchObject([EntityType.Dashboard]);
    });
    it('should return an empty list for downstream lineage for dashboards', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Downstream, EntityType.Dashboard);
        expect(validEntityTypes).toMatchObject([]);
    });
    it('should get valid entity types to query downstream lineage for datajobs', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Downstream, EntityType.DataJob);
        expect(validEntityTypes).toMatchObject([EntityType.DataJob, EntityType.Dataset]);
    });
    it('should return an empty list if the entity type is unexpected for downstream', () => {
        const validEntityTypes = getValidEntityTypes(Direction.Downstream, EntityType.Container);
        expect(validEntityTypes).toMatchObject([]);
    });
});
