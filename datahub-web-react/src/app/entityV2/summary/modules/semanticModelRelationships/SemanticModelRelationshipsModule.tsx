import { Pill } from '@components';
import { ArrowsLeftRight } from '@phosphor-icons/react/dist/csr/ArrowsLeftRight';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    getCardinalityLabelKey,
    getCardinalityPillColor,
    getRelationshipRowKey,
    indexDatasetsByAliasOrName,
} from '@app/entityV2/summary/modules/semanticModelRelationships/utils';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import ModuleEntityIconSlot from '@app/homeV3/module/components/ModuleEntityIconSlot';
import ModuleEntityLink from '@app/homeV3/module/components/ModuleEntityLink';
import ModuleEntityName from '@app/homeV3/module/components/ModuleEntityName';
import ModuleSecondaryText from '@app/homeV3/module/components/ModuleSecondaryText';
import { ModuleProps } from '@app/homeV3/module/types';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Dataset, Entity, EntityType, SemanticModelRelationship } from '@types';

type EntityDataWithRelationships = {
    info?: {
        datasets?: Dataset[] | null;
        relationships?: SemanticModelRelationship[] | null;
    } | null;
};

const RelationshipRow = styled.div`
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
    align-items: center;
    gap: 12px;
    padding: 8px 13px 8px 8px;
`;

const Endpoint = styled.div<{ $align: 'left' | 'right' }>`
    display: flex;
    align-items: center;
    gap: 16px;
    min-width: 0;
    justify-content: ${(props) => (props.$align === 'right' ? 'flex-end' : 'flex-start')};
    text-align: ${(props) => props.$align};
`;

const EndpointText = styled.div<{ $align: 'left' | 'right' }>`
    display: flex;
    flex-direction: column;
    gap: 0;
    min-width: 0;
    align-items: ${(props) => (props.$align === 'right' ? 'flex-end' : 'flex-start')};
`;

const EndpointName = styled(ModuleEntityName)`
    line-height: 20px;

    & span,
    & div {
        line-height: inherit;
    }

    ${Endpoint}:hover & {
        text-decoration: underline;
    }
`;

const CardinalityCell = styled.div`
    display: flex;
    justify-content: center;
    flex-shrink: 0;
`;

type EndpointSideProps = {
    datasetName: string;
    columns: string[];
    source?: Entity | null;
    align: 'left' | 'right';
};

function RelationshipEndpoint({ datasetName, columns, source, align }: EndpointSideProps) {
    const entityRegistry = useEntityRegistryV2();

    const content = (
        <Endpoint $align={align}>
            {source && (
                <ModuleEntityIconSlot>
                    <EntityIcon entity={source} />
                </ModuleEntityIconSlot>
            )}
            <EndpointText $align={align}>
                <EndpointName displayName={datasetName} />
                {columns.map((column) => (
                    <ModuleSecondaryText key={column} ellipsis>
                        {column}
                    </ModuleSecondaryText>
                ))}
            </EndpointText>
        </Endpoint>
    );

    if (source?.urn && source.type === EntityType.Dataset) {
        return <ModuleEntityLink to={entityRegistry.getEntityUrl(source.type, source.urn)}>{content}</ModuleEntityLink>;
    }

    return content;
}

export default function SemanticModelRelationshipsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();

    const typedData = entityData as EntityDataWithRelationships | null;
    const relationships = typedData?.info?.relationships ?? [];

    const datasetsByName = useMemo(
        () => indexDatasetsByAliasOrName(typedData?.info?.datasets ?? []),
        [typedData?.info?.datasets],
    );

    if (!relationships.length) {
        return (
            <LargeModule {...props} dataTestId="semantic-model-relationships-module">
                <EmptyContent
                    icon={ArrowsLeftRight}
                    title={t('semanticModelRelationships.emptyTitle')}
                    description={t('semanticModelRelationships.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} dataTestId="semantic-model-relationships-module">
            {relationships.map((rel, idx) => {
                const fromDataset = datasetsByName.get(rel.from);
                const toDataset = datasetsByName.get(rel.to);
                const label = rel.cardinality ? t(getCardinalityLabelKey(rel.cardinality)) : undefined;
                const color = getCardinalityPillColor(rel.cardinality);

                return (
                    <RelationshipRow key={getRelationshipRowKey(rel, idx)}>
                        <RelationshipEndpoint
                            datasetName={rel.from}
                            columns={rel.fromColumns}
                            source={fromDataset}
                            align="left"
                        />
                        <CardinalityCell>{label && <Pill label={label} color={color} size="sm" />}</CardinalityCell>
                        <RelationshipEndpoint
                            datasetName={rel.to}
                            columns={rel.toColumns}
                            source={toDataset}
                            align="right"
                        />
                    </RelationshipRow>
                );
            })}
        </LargeModule>
    );
}
