import { dataset1, dataset2, dataJob1, dataset1FetchedEntity, dataset2FetchedEntity } from '../../../Mocks';
import { Entity, EntityType } from '../../../types.generated';
import { Direction, EntityAndType, FetchedEntity } from '../types';
import { shouldIncludeChildEntity } from '../utils/constructFetchedNode';

describe('shouldIncludeChildEntity', () => {
    const parentChildren = [
        { entity: dataset1, type: dataset1.type },
        { entity: dataJob1, type: dataJob1.type },
    ] as EntityAndType[];

    it('should return false if parent and child are datasets and the child has a datajob child that belongs to the parent children', () => {
        const shouldBeIncluded = shouldIncludeChildEntity(
            Direction.Upstream,
            parentChildren,
            dataset1FetchedEntity,
            dataset2FetchedEntity,
        );

        expect(shouldBeIncluded).toBe(false);
    });

    it('should return true if the datajob is not a child of the parent', () => {
        const parentChild = [{ entity: dataset1, type: dataset1.type }] as EntityAndType[];
        const shouldBeIncluded = shouldIncludeChildEntity(
            Direction.Upstream,
            parentChild,
            dataset1FetchedEntity,
            dataset2FetchedEntity,
        );

        expect(shouldBeIncluded).toBe(true);
    });

    it('should return true if either parent or child is not a dataset', () => {
        const fetchedDatajobEntity = { ...dataset1FetchedEntity, type: EntityType.DataJob };
        let shouldBeIncluded = shouldIncludeChildEntity(
            Direction.Upstream,
            parentChildren,
            fetchedDatajobEntity,
            dataset2FetchedEntity,
        );
        expect(shouldBeIncluded).toBe(true);

        const fetchedDashboardEntity = { ...dataset2FetchedEntity, type: EntityType.Dashboard };
        shouldBeIncluded = shouldIncludeChildEntity(
            Direction.Upstream,
            parentChildren,
            dataset1FetchedEntity,
            fetchedDashboardEntity,
        );
        expect(shouldBeIncluded).toBe(true);
    });

    it('should return true if the parent has a datajob child that is not a child of the dataset child', () => {
        const updatedDataset1FetchedEntity = {
            ...dataset1FetchedEntity,
            downstreamChildren: [{ type: EntityType.Dataset, entity: dataset2 as Entity }],
        } as FetchedEntity;

        const shouldBeIncluded = shouldIncludeChildEntity(
            Direction.Upstream,
            parentChildren,
            updatedDataset1FetchedEntity,
            dataset2FetchedEntity,
        );

        expect(shouldBeIncluded).toBe(true);
    });
});
