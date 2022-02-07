import { BookOutlined } from '@ant-design/icons';
import { Tag } from 'antd';
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
} from '../../types.generated';
import { StyledTag } from '../entity/shared/components/styled/StyledTag';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { DomainLink } from '../shared/tags/DomainLink';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME } from './utils/constants';

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
        return (
            <>
                <StyledTag $colorHash={tag.urn}>{tag.name}</StyledTag>({countText})
            </>
        );
    }

    if (aggregation.entity?.type === EntityType.GlossaryTerm) {
        const term = aggregation.entity as GlossaryTerm;
        return (
            <>
                <Tag closable={false}>
                    {term.name}
                    <BookOutlined style={{ marginLeft: '2%' }} />
                </Tag>
                ({countText})
            </>
        );
    }

    if (aggregation.entity?.type === EntityType.DataPlatform) {
        const platform = aggregation.entity as DataPlatform;
        return (
            <>
                {!!platform.properties?.logoUrl && (
                    <PreviewImage src={platform.properties?.logoUrl} alt={platform.name} />
                )}
                <span>
                    {platform.properties?.displayName || platform.name} ({countText})
                </span>
            </>
        );
    }

    if (aggregation.entity?.type === EntityType.Container) {
        const container = aggregation.entity as Container;
        return (
            <>
                {!!container.platform?.properties?.logoUrl && (
                    <PreviewImage src={container.platform?.properties?.logoUrl} alt={container.properties?.name} />
                )}
                <span>
                    {container.properties?.name} ({countText})
                </span>
            </>
        );
    }

    if (aggregation.entity?.type === EntityType.Domain) {
        const domain = aggregation.entity as Domain;
        return (
            <>
                <DomainLink urn={domain.urn} name={entityRegistry.getDisplayName(EntityType.Domain, domain)} />
            </>
        );
    }

    // Warning: Special casing for Sub-Types
    if (field === 'typeNames') {
        const subTypeDisplayName = capitalizeFirstLetter(aggregation.value);
        return (
            <>
                <span>
                    {subTypeDisplayName} ({countText})
                </span>
            </>
        );
    }

    return (
        <>
            {aggregation.value} ({countText})
        </>
    );
};
