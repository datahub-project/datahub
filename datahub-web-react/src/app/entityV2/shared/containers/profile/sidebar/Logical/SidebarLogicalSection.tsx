import { colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
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
    color: ${colors.gray[1700]};
`;

const AndMoreWrapper = styled.div`
    margin-left: 4px;
    margin-top: 5px;

    :hover {
        cursor: pointer;
        color: ${colors.primary[500]};
    }
`;

export default function SidebarLogicalSection() {
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const [showAllChildren, setShowAllChildren] = useState(false);
    const { urn, entityData } = useEntityData();

    if (!logicalModelsEnabled) {
        return null;
    }

    let logicalParentSection: JSX.Element | null = null;
    let physicalChildrenSection: JSX.Element | null = null;
    if (entityData?.logicalParent) {
        const { logicalParent } = entityData;
        logicalParentSection = (
            <SidebarSection
                title="Logical Parent"
                infoPopover="Logical Model that defines this asset's metadata.
                Changes to the Logical Parent's metadata propagate automatically to this asset."
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
    if (entityData?.physicalChildren?.total) {
        const physicalChildren = entityData.physicalChildren.relationships
            .map((relationship) => relationship.entity)
            .filter((entity): entity is Entity => !!entity);
        const numNotShown = entityData.physicalChildren.total - physicalChildren.length;
        physicalChildrenSection = (
            <SidebarSection
                title="Physical Children"
                infoPopover="Physical implementations of this Logical Model.
                Changes to this asset's metadata propagate automatically to these children."
                content={
                    <EntityListContainer data-testid="physical-children-list">
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
                        {numNotShown > 0 && (
                            <AndMoreWrapper onClick={() => setShowAllChildren(true)}>
                                and {numNotShown} more
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
                    title="View All Physical Children"
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
