import React from 'react';

import ChildLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildLoader';
import { ChildrenLoaderType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

import { AndFilterInput } from '@types';

interface Props {
    parentValues: string[];
    loadChildren: ChildrenLoaderType;
    loadRelatedEntities?: ChildrenLoaderType;
    relatedEntitiesOrFilters?: AndFilterInput[] | undefined;
}

export default function ChildrenLoader({
    parentValues,
    loadChildren,
    loadRelatedEntities,
    relatedEntitiesOrFilters,
}: Props) {
    return (
        <>
            {parentValues.map((parentValue) => (
                <ChildLoader
                    parentValue={parentValue}
                    key={parentValue}
                    loadChildren={loadChildren}
                    loadRelatedEntities={loadRelatedEntities}
                    relatedEntitiesOrFilters={relatedEntitiesOrFilters}
                />
            ))}
        </>
    );
}
