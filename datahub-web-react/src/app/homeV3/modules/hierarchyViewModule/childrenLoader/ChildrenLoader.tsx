import React from 'react';

import ChildLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildLoader';
import { ChildrenLoaderType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

interface Props {
    parentValues: string[];
    loadChildren: ChildrenLoaderType;
    loadRelatedEntities?: ChildrenLoaderType;
}

export default function ChildrenLoader({ parentValues, loadChildren, loadRelatedEntities }: Props) {
    return (
        <>
            {parentValues.map((parentValue) => (
                <ChildLoader
                    parentValue={parentValue}
                    key={parentValue}
                    loadChildren={loadChildren}
                    loadRelatedEntities={loadRelatedEntities}
                />
            ))}
        </>
    );
}
