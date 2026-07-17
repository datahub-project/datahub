import { Pill, Text, radius } from '@components';
import { ColorOptions } from '@components/theme/config';
import { ArrowsLeftRight } from '@phosphor-icons/react/dist/csr/ArrowsLeftRight';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { PlatformIcon } from '@app/searchV2/autoCompleteV2/components/icon/PlatformIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import {
    DataPlatform,
    Dataset,
    Entity,
    EntityType,
    ErModelRelationshipCardinality,
    ModelDataset,
    SemanticModelRelationship,
} from '@types';

type EntityDataWithRelationships = {
    platform?: DataPlatform | null;
    info?: {
        datasets?: ModelDataset[] | null;
        relationships?: SemanticModelRelationship[] | null;
    } | null;
};

const RelationshipRow = styled.div`
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
    gap: 12px;
    padding: 10px 4px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};

    &:last-child {
        border-bottom: none;
    }
`;

const Endpoint = styled.div<{ $align: 'left' | 'right' }>`
    display: flex;
    align-items: flex-start;
    gap: 8px;
    min-width: 0;
    justify-content: ${(props) => (props.$align === 'right' ? 'flex-end' : 'flex-start')};
    text-align: ${(props) => props.$align};
`;

const IconContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${(props) => props.theme.colors.bgSurface};
    height: 28px;
    width: 28px;
    border-radius: ${radius.full};
    flex-shrink: 0;
`;

const EndpointText = styled.div<{ $align: 'left' | 'right' }>`
    display: flex;
    flex-direction: column;
    min-width: 0;
    align-items: ${(props) => (props.$align === 'right' ? 'flex-end' : 'flex-start')};
`;

const DatasetName = styled(Text).attrs({ size: 'sm', weight: 'semiBold' })`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
`;

const ColumnName = styled(Text).attrs({ size: 'xs', color: 'gray' })`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
`;

const CardinalityCell = styled.div`
    display: flex;
    justify-content: center;
    flex-shrink: 0;
`;

const StyledLink = styled(Link)`
    display: block;
    color: inherit;
    text-decoration: none;
    min-width: 0;

    &:hover ${DatasetName} {
        text-decoration: underline;
    }
`;

const PLATFORM_ICON_SIZE = 16;

const CARDINALITY_LABEL_KEYS: Record<ErModelRelationshipCardinality, string> = {
    [ErModelRelationshipCardinality.OneOne]: 'relationships.cardinality.oneOne',
    [ErModelRelationshipCardinality.NOne]: 'relationships.cardinality.manyOne',
    [ErModelRelationshipCardinality.OneN]: 'relationships.cardinality.oneMany',
    [ErModelRelationshipCardinality.NN]: 'relationships.cardinality.manyMany',
};

const CARDINALITY_LABELS_FALLBACK: Record<ErModelRelationshipCardinality, string> = {
    [ErModelRelationshipCardinality.OneOne]: 'One → One',
    [ErModelRelationshipCardinality.NOne]: 'Many → One',
    [ErModelRelationshipCardinality.OneN]: 'One → Many',
    [ErModelRelationshipCardinality.NN]: 'Many → Many',
};

const CARDINALITY_COLORS: Record<ErModelRelationshipCardinality, ColorOptions> = {
    [ErModelRelationshipCardinality.OneOne]: 'blue',
    [ErModelRelationshipCardinality.NOne]: 'violet',
    [ErModelRelationshipCardinality.OneN]: 'green',
    [ErModelRelationshipCardinality.NN]: 'yellow',
};

const DEFAULT_PILL_COLOR: ColorOptions = 'gray';

type EndpointSideProps = {
    datasetName: string;
    columns: string[];
    platform?: DataPlatform | null;
    source?: Entity | null;
    align: 'left' | 'right';
};

function RelationshipEndpoint({ datasetName, columns, platform, source, align }: EndpointSideProps) {
    const entityRegistry = useEntityRegistryV2();

    const content = (
        <Endpoint $align={align}>
            {platform && (
                <IconContainer>
                    <PlatformIcon platform={platform} size={PLATFORM_ICON_SIZE} />
                </IconContainer>
            )}
            <EndpointText $align={align}>
                <DatasetName>{datasetName}</DatasetName>
                {columns.map((column) => (
                    <ColumnName key={column}>{column}</ColumnName>
                ))}
            </EndpointText>
        </Endpoint>
    );

    if (source?.urn && source.type === EntityType.Dataset) {
        return <StyledLink to={entityRegistry.getEntityUrl(source.type, source.urn)}>{content}</StyledLink>;
    }

    return content;
}

export default function RelationshipsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();

    const typedData = entityData as EntityDataWithRelationships | null;
    const relationships = typedData?.info?.relationships ?? [];
    const fallbackPlatform = typedData?.platform;

    const datasetsByName = useMemo(() => {
        const map = new Map<string, ModelDataset>();
        (typedData?.info?.datasets ?? []).forEach((dataset) => map.set(dataset.name, dataset));
        return map;
    }, [typedData?.info?.datasets]);

    if (!relationships.length) {
        return (
            <LargeModule {...props} dataTestId="relationships-module">
                <EmptyContent
                    icon={ArrowsLeftRight}
                    title={t('relationships.emptyTitle')}
                    description={t('relationships.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} dataTestId="relationships-module">
            {relationships.map((rel, idx) => {
                const fromDataset = datasetsByName.get(rel.from);
                const toDataset = datasetsByName.get(rel.to);
                const fromSource = fromDataset?.source as Dataset | undefined;
                const toSource = toDataset?.source as Dataset | undefined;
                const fromPlatform = fromSource?.platform ?? fallbackPlatform;
                const toPlatform = toSource?.platform ?? fallbackPlatform;

                const label = rel.cardinality
                    ? t(CARDINALITY_LABEL_KEYS[rel.cardinality], CARDINALITY_LABELS_FALLBACK[rel.cardinality])
                    : undefined;
                const color = rel.cardinality ? CARDINALITY_COLORS[rel.cardinality] : DEFAULT_PILL_COLOR;

                return (
                    // eslint-disable-next-line react/no-array-index-key
                    <RelationshipRow key={rel.name ?? `${rel.from}-${rel.to}-${idx}`}>
                        <RelationshipEndpoint
                            datasetName={rel.from}
                            columns={rel.fromColumns}
                            platform={fromPlatform}
                            source={fromSource}
                            align="left"
                        />
                        <CardinalityCell>
                            {label && <Pill label={label} color={color} size="sm" />}
                        </CardinalityCell>
                        <RelationshipEndpoint
                            datasetName={rel.to}
                            columns={rel.toColumns}
                            platform={toPlatform}
                            source={toSource}
                            align="right"
                        />
                    </RelationshipRow>
                );
            })}
        </LargeModule>
    );
}
