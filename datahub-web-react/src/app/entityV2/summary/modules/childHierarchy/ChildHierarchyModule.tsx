import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getChildHierarchyModule } from '@app/entityV2/summary/modules/childHierarchy/utils';
import { ModuleProps } from '@app/homeV3/module/types';
import HierarchyViewModule from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModule';

export default function ChildHierarchyModule(props: ModuleProps) {
    const { urn, entityType } = useEntityData();
    const module = getChildHierarchyModule(props.module, urn, entityType);

    return <HierarchyViewModule {...props} module={module} />;
}
