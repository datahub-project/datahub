import { BookOutlined } from '@ant-design/icons';
import { Tag, Tooltip } from 'antd';
import * as React from 'react';
import styled from 'styled-components';
import {
    AggregationMetadata,
    Domain,
    Container,
    DataPlatform,
    EntityType,
    GlossaryTerm,
    Tag as TagType,
    CorpUser,
    CorpGroup,
} from '../../types.generated';
import { StyledTag } from '../entity/shared/components/styled/StyledTag';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { DomainLink } from '../shared/tags/DomainLink';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME } from './utils/constants';
import CustomAvatar from '../shared/avatar/CustomAvatar';
import { IconStyleType } from '../entity/Entity';

type Props = {
    aggregation: AggregationMetadata;
    field: string;
};

const PreviewImage = styled.img`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
`;

const MAX_COUNT_VAL = 10000;

// SearchFilterLabel renders custom labels for entity, tag, term & data platform filters. All other filters use the default behavior.
export const SearchFilterLabel = ({ aggregation, field }: Props) => {
    const entityRegistry = useEntityRegistry();
    const countText = aggregation.count === MAX_COUNT_VAL ? '10000+' : aggregation.count;

    if (field === ENTITY_FILTER_NAME) {
        const entityType = aggregation.value.toUpperCase() as EntityType;
        return (
            <span>
                {entityType ? entityRegistry.getCollectionName(entityType) : aggregation.value} ({countText})
            </span>
        );
    }

    if (aggregation.entity?.type === EntityType.Tag) {
        const tag = aggregation.entity as TagType;
        const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <StyledTag $colorHash={tag?.urn} $color={tag?.properties?.colorHex}>
                    {truncatedDisplayName}
                </StyledTag>
                ({countText})
            </Tooltip>
        );
    }

    if (aggregation.entity?.type === EntityType.CorpUser) {
        const user = aggregation.entity as CorpUser;
        const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <CustomAvatar
                    size={18}
                    name={truncatedDisplayName}
                    photoUrl={user.editableProperties?.pictureLink || undefined}
                    useDefaultAvatar={false}
                    style={{
                        marginRight: 8,
                    }}
                />
                {displayName} ({countText})
            </Tooltip>
        );
    }

    if (aggregation.entity?.type === EntityType.CorpGroup) {
        const group = aggregation.entity as CorpGroup;
        const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <span style={{ marginRight: 8 }}>
                    {entityRegistry.getIcon(EntityType.CorpGroup, 16, IconStyleType.ACCENT)}
                </span>
                {truncatedDisplayName} ({countText})
            </Tooltip>
        );
    }

    if (aggregation.entity?.type === EntityType.GlossaryTerm) {
        const term = aggregation.entity as GlossaryTerm;
        const displayName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <Tag closable={false}>
                    <BookOutlined style={{ marginRight: '3%' }} />
                    {truncatedDisplayName}
                </Tag>
                ({countText})
            </Tooltip>
        );
    }

    if (aggregation.entity?.type === EntityType.DataPlatform) {
        const platform = aggregation.entity as DataPlatform;
        const displayName = platform.properties?.displayName || platform.info?.displayName || platform.name;
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!platform.properties?.logoUrl && (
                    <PreviewImage src={platform.properties?.logoUrl} alt={platform.name} />
                )}
                <span>
                    {truncatedDisplayName} ({countText})
                </span>
            </Tooltip>
        );
    }

    if (aggregation.entity?.type === EntityType.Container) {
        const container = aggregation.entity as Container;
        const displayName = entityRegistry.getDisplayName(EntityType.Container, container);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!container.platform?.properties?.logoUrl && (
                    <PreviewImage src={container.platform?.properties?.logoUrl} alt={container.properties?.name} />
                )}
                <span>
                    {truncatedDisplayName} ({countText})
                </span>
            </Tooltip>
        );
    }

    if (aggregation.entity?.type === EntityType.Domain) {
        const domain = aggregation.entity as Domain;
        const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
        const truncatedDomainName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <DomainLink urn={domain.urn} name={truncatedDomainName} />({countText})
            </Tooltip>
        );
    }

    // Warning: Special casing for Sub-Types
    if (field === 'typeNames') {
        const displayName = capitalizeFirstLetter(aggregation.value) || '';
        const truncatedDomainName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <span>
                    {truncatedDomainName} ({countText})
                </span>
            </Tooltip>
        );
    }

    if (field === 'degree') {
        return <>{aggregation.value}</>;
    }
    return (
        <>
            {aggregation.value} ({countText})
        </>
    );
};
