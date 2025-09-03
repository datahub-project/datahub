import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ModuleProps } from '@app/homeV3/module/types';
import HierarchyViewModule from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModule';

export default function ChildHierarchyModule(props: ModuleProps) {
    const { urn } = useEntityData();
    const module = {
        ...props.module,
        properties: {
            ...props.module.properties,
            params: {
                ...props.module.properties.params,
                hierarchyViewParams: { assetUrns: [urn], showRelatedEntities: true },
            },
        },
    };

    return <HierarchyViewModule {...props} module={module} />;
}
