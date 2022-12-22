import * as React from 'react';
import { BookOutlined } from '@ant-design/icons';
import { Tag, Tooltip } from 'antd';
import styled from 'styled-components';
import {
    Domain,
    Container,
    DataPlatform,
    EntityType,
    GlossaryTerm,
    Tag as TagType,
    CorpUser,
    CorpGroup,
    DataPlatformInstance,
    Entity,
} from '../../types.generated';
import { StyledTag } from '../entity/shared/components/styled/StyledTag';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { DomainLink } from '../shared/tags/DomainLink';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME } from './utils/constants';
import CustomAvatar from '../shared/avatar/CustomAvatar';
import { IconStyleType } from '../entity/Entity';
import { formatNumber } from '../shared/formatNumber';

type Props = {
    field: string;
    value: string;
    count?: number;
    entity?: Entity | null; // TODO: If the entity is not provided, we should hydrate it.
    hideCount?: boolean;
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
export const SearchFilterLabel = ({ field, value, entity, count, hideCount }: Props) => {
    const entityRegistry = useEntityRegistry();
    const countText = hideCount ? '' : ` (${count === MAX_COUNT_VAL ? '10k+' : formatNumber(count)})`;

    if (field === ENTITY_FILTER_NAME) {
        const entityType = value.toUpperCase() as EntityType;
        return (
            <span>
                {entityType ? entityRegistry.getCollectionName(entityType) : value}
                {countText}
            </span>
        );
    }

    if (entity?.type === EntityType.Tag) {
        const tag = entity as TagType;
        const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <StyledTag $colorHash={tag?.urn} $color={tag?.properties?.colorHex}>
                    {truncatedDisplayName}
                </StyledTag>
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.CorpUser) {
        const user = entity as CorpUser;
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
                {displayName}
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.CorpGroup) {
        const group = entity as CorpGroup;
        const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <span style={{ marginRight: 8 }}>
                    {entityRegistry.getIcon(EntityType.CorpGroup, 16, IconStyleType.ACCENT)}
                </span>
                {truncatedDisplayName}
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.GlossaryTerm) {
        const term = entity as GlossaryTerm;
        const displayName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <Tag closable={false}>
                    <BookOutlined style={{ marginRight: '3%' }} />
                    {truncatedDisplayName}
                </Tag>
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.DataPlatform) {
        const platform = entity as DataPlatform;
        const displayName = platform.properties?.displayName || platform.info?.displayName || platform.name;
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!platform.properties?.logoUrl && (
                    <PreviewImage src={platform.properties?.logoUrl} alt={platform.name} />
                )}
                <span>
                    {truncatedDisplayName}
                    {countText}
                </span>
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.DataPlatformInstance) {
        const platform = entity as DataPlatformInstance;
        const displayName = platform.instanceId;
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {truncatedDisplayName}
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.Container) {
        const container = entity as Container;
        const displayName = entityRegistry.getDisplayName(EntityType.Container, container);
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!container.platform?.properties?.logoUrl && (
                    <PreviewImage src={container.platform?.properties?.logoUrl} alt={container.properties?.name} />
                )}
                <span>
                    {truncatedDisplayName}
                    {countText}
                </span>
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.Domain) {
        const domain = entity as Domain;
        const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
        const truncatedDomainName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <DomainLink domain={domain} name={truncatedDomainName} />
                {countText}
            </Tooltip>
        );
    }

    // Warning: Special casing for Sub-Types
    if (field === 'typeNames') {
        const displayName = capitalizeFirstLetter(value) || '';
        const truncatedDomainName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                <span>
                    {truncatedDomainName}
                    {countText}
                </span>
            </Tooltip>
        );
    }

    if (field === 'degree') {
        return <>{value}</>;
    }
    return (
        <>
            {value}
            {countText}
        </>
    );
};
