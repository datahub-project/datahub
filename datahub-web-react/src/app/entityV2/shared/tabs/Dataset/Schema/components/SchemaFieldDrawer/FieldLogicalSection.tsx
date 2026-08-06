import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
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
    color: ${(props) => props.theme.colors.textSecondary};
`;

const AndMoreWrapper = styled.div`
    margin-left: 4px;
    margin-top: 5px;

    :hover {
        cursor: pointer;
        color: ${(props) => props.theme.colors.textHover};
    }
`;

interface Props {
    expandedField: SchemaField;
}

export default function FieldLogicalSection({ expandedField }: Props) {
    const { t } = useTranslation('entity.profile.schema');
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
                title={t('fieldLogical.logicalParentTitle')}
                infoPopover={t('fieldLogical.logicalParentInfo')}
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
                title={t('fieldLogical.physicalChildrenTitle')}
                infoPopover={t('fieldLogical.physicalChildrenInfo')}
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
                                {t('fieldLogical.andMore', { count: numNotShown })}
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
                    title={t('fieldLogical.viewAllPhysicalChildren')}
                    fixedFilters={{
                        unionType: UnionType.OR,
                        // eslint-disable-next-line i18next/no-literal-string -- backend filter field identifier, not UI text
                        filters: [{ field: 'logicalParent', values: [urn] }],
                    }}
                    onClose={() => setShowAllChildren(false)}
                />
            )}
            {!!(logicalParentSection || physicalChildrenSection) && <StyledDivider />}
        </>
    );
}
