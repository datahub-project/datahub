import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getChildHierarchyModule } from '@app/entityV2/summary/modules/childHierarchy/utils';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import HierarchyViewModule from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModule';

import { EntityType } from '@types';

export default function ChildHierarchyModule(props: ModuleProps) {
    const { urn, entityType, entityData } = useEntityData();
    const module = getChildHierarchyModule(props.module, urn, entityType);

    const isEmpty = entityType === EntityType.Domain && (entityData?.children?.total || 0) === 0;

    if (isEmpty) {
        return (
            <LargeModule {...props} module={module} dataTestId="hierarchy-module">
                <EmptyContent
                    icon="Stack"
                    title="No Child Domains"
                    description="This domain has no child domains. Add children to see them in this module."
                />
            </LargeModule>
        );
    }

    return <HierarchyViewModule {...props} module={module} showViewAll={false} />;
}
