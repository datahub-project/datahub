import useLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useLoader';
import { ChildrenLoaderType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

import { AndFilterInput } from '@types';

interface Props {
    parentValue: string;
    loadChildren: ChildrenLoaderType;
    loadRelatedEntities?: ChildrenLoaderType;
    relatedEntitiesOrFilters?: AndFilterInput[] | undefined;
}

export default function ChildLoader({
    parentValue,
    loadChildren,
    loadRelatedEntities,
    relatedEntitiesOrFilters,
}: Props) {
    useLoader(parentValue, loadChildren, loadRelatedEntities, relatedEntitiesOrFilters);

    return null;
}
