import useRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useRelatedEntitiesLoader';
import {
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

export default function useGlossaryRelatedEntitiesLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
    dependenciesIsLoading,
}: ChildrenLoaderInputType): ChildrenLoaderResultType {
    return useRelatedEntitiesLoader({
        parentValue,
        metadata,
        maxNumberToLoad,
        dependenciesIsLoading,
        orFilters: [
            {
                and: [{ field: 'glossaryTerms', values: [parentValue] }],
            },
            {
                and: [{ field: 'fieldGlossaryTerms', values: [parentValue] }],
            },
        ],
    });
}
