import { EditOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { Button, message } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import UpdateDescriptionModal from '@app/entity/shared/components/legacy/DescriptionModal';
import DescriptionSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import PropagationDetails from '@app/entity/shared/propagation/PropagationDetails';
import { useSchemaRefetch } from '@app/entity/shared/tabs/Dataset/Schema/SchemaContext';
import {
    SectionHeader,
    StyledDivider,
} from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import { getFieldDescriptionDetails } from '@app/entity/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { UnionType } from '@app/searchV2/utils/constants';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { useGetSchemaFieldQuery } from '@graphql/schemaField.generated';
import { EditableSchemaFieldInfo, Entity, SchemaField, SubResourceType } from '@types';

const Wrapper = styled.div`
    display: flex;
    gap: 4px;
    align-items: center;
    justify-content: space-between;
`;

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
    const [showAllChildren, setShowAllChildren] = useState(false);
    const urn = expandedField.schemaFieldEntity?.urn;

    const { data } = useGetSchemaFieldQuery({
        variables: { urn: urn || '' },
        skip: !urn,
    });

    if (!urn || data?.entity?.__typename !== 'SchemaFieldEntity') {
        return null;
    }

    let logicalParentSection: JSX.Element | null = null;
    let physicalChildrenSection: JSX.Element | null = null;
    if (data.entity.logicalParent) {
        const logicalParent = data.entity.logicalParent;
        logicalParentSection = (
            <SidebarSection
                title="Logical Parent"
                content={<CompactEntityNameComponent key={logicalParent.urn} entity={logicalParent} showFullTooltip />}
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
                content={
                    <EntityListContainer data-testid="physical-children-list">
                        <CompactEntityNameList entities={physicalChildren} showFullTooltips />
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
