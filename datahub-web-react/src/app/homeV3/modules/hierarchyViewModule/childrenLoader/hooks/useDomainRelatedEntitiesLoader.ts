import useRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useRelatedEntitiesLoader';
import {
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

export default function useDomainRelatedEntitiesLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
}: ChildrenLoaderInputType): ChildrenLoaderResultType {
    return useRelatedEntitiesLoader({
        parentValue,
        metadata,
        maxNumberToLoad,
        orFilters: [
            {
                and: [{ field: 'domains', values: [parentValue] }],
            },
        ],
    });
}
