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
import { capitalizeFirstLetterOnly } from '../shared/textUtil';
import { DomainLink } from '../shared/tags/DomainLink';
import { useEntityRegistry } from '../useEntityRegistry';
import { BROWSE_PATH_V2_FILTER_NAME, ENTITY_FILTER_NAME, MAX_COUNT_VAL } from './utils/constants';
import CustomAvatar from '../shared/avatar/CustomAvatar';
import { IconStyleType } from '../entity/Entity';
import { formatNumber } from '../shared/formatNumber';
import useGetBrowseV2LabelOverride from './filters/useGetBrowseV2LabelOverride';
import { getParentEntities } from './filters/utils';
import { ParentWrapper } from '../entity/shared/containers/profile/sidebar/Container/ContainerSelectModal';
import ParentEntities from './filters/ParentEntities';

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

// SearchFilterLabel renders custom labels for entity, tag, term & data platform filters. All other filters use the default behavior.
export const SearchFilterLabel = ({ field, value, entity, count, hideCount }: Props) => {
    const entityRegistry = useEntityRegistry();
    const filterLabelOverride = useGetBrowseV2LabelOverride(field, value, entityRegistry);
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
                <StyledTag $colorHash={tag?.urn} $color={tag?.properties?.colorHex} fontSize={10}>
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
                    <BookOutlined style={{ marginRight: '4px' }} />
                    {truncatedDisplayName}
                </Tag>
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.DataPlatform) {
        const platform = entity as DataPlatform;
        const displayName =
            platform.properties?.displayName ||
            platform.info?.displayName ||
            capitalizeFirstLetterOnly(platform.name) ||
            '';
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!platform.properties?.logoUrl && (
                    <PreviewImage src={platform.properties?.logoUrl} alt={displayName} />
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
        const parentEntities: Entity[] = getParentEntities(container as Entity) || [];
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!container.platform?.properties?.logoUrl && (
                    <>
                        <ParentWrapper style={{ width: '200px' }}>
                            <ParentEntities parentEntities={parentEntities} />
                        </ParentWrapper>
                        <PreviewImage src={container.platform?.properties?.logoUrl} alt={container.properties?.name} />
                    </>
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
                <DomainLink domain={domain} name={truncatedDomainName} tagStyle={{ fontSize: 10 }} fontSize={10} />
                {countText}
            </Tooltip>
        );
    }

    if (entity?.type === EntityType.DataProduct) {
        const displayName = entityRegistry.getDisplayName(EntityType.DataProduct, entity);
        const truncatedName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {truncatedName}
                {countText}
            </Tooltip>
        );
    }

    // Warning: Special casing for Sub-Types
    if (field === 'typeNames') {
        const displayName = capitalizeFirstLetterOnly(value) || '';
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

    if (field === BROWSE_PATH_V2_FILTER_NAME) {
        return <>{filterLabelOverride || value}</>;
    }

    return (
        <>
            {value}
            {countText}
        </>
    );
};
