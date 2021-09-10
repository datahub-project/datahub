import { BookOutlined } from '@ant-design/icons';
import { Image, Tag } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

import { AggregationMetadata, DataPlatform, EntityType, GlossaryTerm, Tag as TagType } from '../../types.generated';
import { StyledTag } from '../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME } from './utils/constants';

type Props = {
    aggregation: AggregationMetadata;
    field: string;
};

const PreviewImage = styled(Image)`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    margin-right: 10px;
    background-color: transparent;
`;

export const SearchFilterLabel = ({ aggregation, field }: Props) => {
    const entityRegistry = useEntityRegistry();

    if (field === ENTITY_FILTER_NAME) {
        return (
            <span>
                {entityRegistry.getCollectionName(aggregation.value.toUpperCase() as EntityType)} ({aggregation.count})
            </span>
        );
    }

    if (aggregation.entity?.type === EntityType.Tag) {
        const tag = aggregation.entity as TagType;
        return (
            <>
                <StyledTag $colorHash={tag.urn}>{tag.name}</StyledTag>({aggregation.count})
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
                ({aggregation.count})
            </>
        );
    }

    if (aggregation.entity?.type === EntityType.DataPlatform) {
        const platform = aggregation.entity as DataPlatform;
        return (
            <>
                {!!platform.info?.logoUrl && <PreviewImage src={platform.info?.logoUrl} alt={platform.name} />}
                {platform.name} ({aggregation.count})
            </>
        );
    }

    return (
        <>
            {aggregation.value} ({aggregation.count})
        </>
    );
};
