import { Stack } from '@phosphor-icons/react/dist/csr/Stack';
import React from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('modules');
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
                    icon={Stack}
                    title={t('childHierarchy.emptyTitle')}
                    description={t('childHierarchy.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return <HierarchyViewModule {...props} module={module} showViewAll={false} />;
}
