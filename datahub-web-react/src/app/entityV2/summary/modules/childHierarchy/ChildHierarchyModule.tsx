/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getChildHierarchyModule } from '@app/entityV2/summary/modules/childHierarchy/utils';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import HierarchyViewModule from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModule';

import { useGetDomainChildrenCountQuery } from '@graphql/domain.generated';
import { EntityType } from '@types';

export default function ChildHierarchyModule(props: ModuleProps) {
    const { urn, entityType } = useEntityData();
    const module = getChildHierarchyModule(props.module, urn, entityType);
    const { isReloading } = useModuleContext();

    const { data: domainChildrenCountData } = useGetDomainChildrenCountQuery({
        variables: { urn },
        skip: entityType !== EntityType.Domain,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
    });

    const isEmpty = entityType === EntityType.Domain && (domainChildrenCountData?.domain?.children?.total ?? 0) === 0;

    if (isEmpty) {
        return (
            <LargeModule {...props} module={module} dataTestId="hierarchy-module">
                <EmptyContent
                    icon="Stack"
                    title="No Domains"
                    description="This domain has no children domains. Add domains to see them in this module."
                />
            </LargeModule>
        );
    }

    return <HierarchyViewModule {...props} module={module} showViewAll={false} />;
}
