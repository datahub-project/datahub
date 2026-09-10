import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import AddPhysicalChildAction from '@app/entityV2/shared/containers/profile/sidebar/Logical/AddPhysicalChildAction';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import UnlinkPhysicalChildButton from '@app/entityV2/shared/logicalModels/UnlinkPhysicalChildButton';
import { isLogicalModel as isLogicalModelEntity } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { UnionType } from '@app/searchV2/utils/constants';
import { useAppConfig } from '@app/useAppConfig';

import { Entity } from '@types';

const EntityListContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;

    margin-left: -8px;
    color: ${(props) => props.theme.colors.text};
`;

const AndMoreWrapper = styled.div`
    margin-left: 4px;
    margin-top: 5px;

    :hover {
        cursor: pointer;
        color: ${(props) => props.theme.colors.textBrand};
    }
`;

const ChildRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
`;

export default function SidebarLogicalSection() {
    const { t } = useTranslation('entity.shared.containers');
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const [showAllChildren, setShowAllChildren] = useState(false);
    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();

    if (!logicalModelsEnabled) {
        return null;
    }

    const isLogicalModel = isLogicalModelEntity(entityType, entityData);
    const hasPhysicalChildren = !!entityData?.physicalChildren?.total;
    // Link/unlink writes are authorized at the write layer (LogicalParentAuthorizationValidator:
    // Edit on both child and parent); canEditProperties is the closest per-entity pre-check for
    // this entity's side, so the edit affordances are hidden from users who can't edit anyway.
    const canEditChildren = isLogicalModel && !!entityData?.privileges?.canEditProperties;

    let logicalParentSection: JSX.Element | null = null;
    let physicalChildrenSection: JSX.Element | null = null;
    if (entityData?.logicalParent) {
        const { logicalParent } = entityData;
        logicalParentSection = (
            <SidebarSection
                title={t('sidebar.logical.parentTitle')}
                infoPopover={t('sidebar.logical.parentInfo')}
                content={
                    <CompactEntityNameComponent
                        key={logicalParent.urn}
                        entity={logicalParent}
                        onClick={() =>
                            analytics.event({
                                type: EventType.GoToLogicalParentEvent,
                                entityUrn: urn,
                                parentUrn: logicalParent.urn,
                            })
                        }
                        showFullTooltip
                    />
                }
            />
        );
    }
    if (isLogicalModel || hasPhysicalChildren) {
        const physicalChildren = (entityData?.physicalChildren?.relationships ?? [])
            .map((relationship) => relationship.entity)
            .filter((entity): entity is Entity => !!entity);
        const numNotShown = (entityData?.physicalChildren?.total ?? 0) - physicalChildren.length;
        physicalChildrenSection = (
            <SidebarSection
                title={t('sidebar.logical.childrenTitle')}
                infoPopover={t('sidebar.logical.childrenInfo')}
                extra={
                    canEditChildren ? <AddPhysicalChildAction logicalParentUrn={urn} onLinked={refetch} /> : undefined
                }
                content={
                    <EntityListContainer data-testid="physical-children-list">
                        {canEditChildren ? (
                            physicalChildren.map((child, index) => (
                                <ChildRow key={child.urn} style={{ width: '100%' }}>
                                    <CompactEntityNameComponent
                                        entity={child}
                                        onClick={() =>
                                            analytics.event({
                                                type: EventType.GoToPhysicalChildEvent,
                                                entityUrn: urn,
                                                childUrn: physicalChildren[index]?.urn,
                                            })
                                        }
                                        showFullTooltip
                                    />
                                    <UnlinkPhysicalChildButton childUrn={child.urn} onUnlinked={refetch} />
                                </ChildRow>
                            ))
                        ) : (
                            <CompactEntityNameList
                                entities={physicalChildren}
                                onClick={(index) =>
                                    analytics.event({
                                        type: EventType.GoToPhysicalChildEvent,
                                        entityUrn: urn,
                                        childUrn: physicalChildren[index]?.urn,
                                    })
                                }
                                showFullTooltips
                            />
                        )}
                        {numNotShown > 0 && (
                            <AndMoreWrapper onClick={() => setShowAllChildren(true)}>
                                {t('sidebar.logical.andMoreCount', { count: numNotShown })}
                            </AndMoreWrapper>
                        )}
                    </EntityListContainer>
                }
            />
        );
    }

    return (
        <>
            {logicalParentSection}
            {physicalChildrenSection}
            {showAllChildren && (
                <EmbeddedListSearchModal
                    title={t('sidebar.logical.viewAllChildren')}
                    fixedFilters={{
                        unionType: UnionType.OR,
                        filters: [{ field: 'logicalParent', values: [urn] }],
                    }}
                    onClose={() => setShowAllChildren(false)}
                />
            )}
        </>
    );
}
