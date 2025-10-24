import { colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { StyledDivider } from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { UnionType } from '@app/searchV2/utils/constants';
import { useAppConfig } from '@app/useAppConfig';

import { useGetSchemaFieldQuery } from '@graphql/schemaField.generated';
import { Entity, SchemaField } from '@types';

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

interface Props {
    expandedField: SchemaField;
}

export default function FieldLogicalSection({ expandedField }: Props) {
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const [showAllChildren, setShowAllChildren] = useState(false);
    const urn = expandedField.schemaFieldEntity?.urn;

    const { data } = useGetSchemaFieldQuery({
        variables: { urn: urn || '' },
        skip: !urn || !logicalModelsEnabled,
    });

    if (!logicalModelsEnabled || !urn || data?.entity?.__typename !== 'SchemaFieldEntity') {
        return null;
    }

    let logicalParentSection: JSX.Element | null = null;
    let physicalChildrenSection: JSX.Element | null = null;
    if (data.entity.logicalParent) {
        const { logicalParent } = data.entity;
        logicalParentSection = (
            <SidebarSection
                title="Logical Parent"
                infoPopover="Column in the Logical Model that defines this column's metadata.
                Changes to the Logical Parent's column metadata propagate automatically to this column."
                content={
                    <CompactEntityNameComponent
                        key={logicalParent.urn}
                        entity={logicalParent}
                        onClick={() =>
                            analytics.event({
                                type: EventType.GoToLogicalParentColumnEvent,
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
    if (data.entity.physicalChildren?.total) {
        const physicalChildren = data.entity.physicalChildren.relationships
            .map((relationship) => relationship.entity)
            .filter((entity): entity is Entity => !!entity);
        const numNotShown = data.entity.physicalChildren.total - physicalChildren.length;
        physicalChildrenSection = (
            <SidebarSection
                title="Physical Children"
                infoPopover="Physical implementations of this Logical Model column.
                Changes to this column's metadata propagate automatically to these child columns."
                content={
                    <EntityListContainer data-testid="physical-children-list">
                        <CompactEntityNameList
                            entities={physicalChildren}
                            onClick={(index) =>
                                analytics.event({
                                    type: EventType.GoToPhysicalChildColumnEvent,
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
            {!!(logicalParentSection || physicalChildrenSection) && <StyledDivider />}
        </>
    );
}
