import { Text, Tooltip } from '@components';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    getSemanticModelDatasetDescription,
    getSemanticModelDatasetDisplayName,
    withSemanticModelAlias,
} from '@app/entityV2/summary/modules/shared/semanticModelDatasetUtils';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Dataset, Entity } from '@types';

type EntityDataWithDatasets = {
    info?: {
        datasets?: Dataset[] | null;
    } | null;
};

const HoverContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-width: 320px;
    padding: 2px 0;
    color: ${(props) => props.theme.colors.text};
`;

const HoverSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
`;

const HoverLabel = styled(Text).attrs({ size: 'xs', weight: 'semiBold' })`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const HoverValue = styled(Text).attrs({ size: 'sm' })`
    color: ${(props) => props.theme.colors.text};
`;

type DatasetHoverProps = {
    alias?: string | null;
    displayName: string;
    description?: string;
};

function DatasetHoverCard({ alias, displayName, description }: DatasetHoverProps) {
    const { t } = useTranslation('modules');

    return (
        <HoverContent>
            {alias && (
                <HoverSection>
                    <HoverLabel>{t('semanticModelDatasets.aliasLabel', 'Alias')}</HoverLabel>
                    <HoverValue>{alias}</HoverValue>
                </HoverSection>
            )}
            <HoverSection>
                <HoverLabel>{t('semanticModelDatasets.nameLabel', 'Name')}</HoverLabel>
                <HoverValue>{displayName}</HoverValue>
            </HoverSection>
            {description && (
                <HoverSection>
                    <HoverLabel>{t('semanticModelDatasets.descriptionLabel', 'Description')}</HoverLabel>
                    <HoverValue>{description}</HoverValue>
                </HoverSection>
            )}
        </HoverContent>
    );
}

export default function SemanticModelDatasetsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();

    const datasets = (entityData as EntityDataWithDatasets)?.info?.datasets ?? [];

    const renderType = useCallback(
        (entity: Entity) => {
            const typeName = entityRegistry.getEntityName(entity.type) ?? entity.type;
            return <Text size="sm">{typeName}</Text>;
        },
        [entityRegistry],
    );

    const renderHover = useCallback((dataset: Dataset, children: React.ReactNode) => {
        return (
            <Tooltip
                title={
                    <DatasetHoverCard
                        alias={dataset.semanticModelProperties?.alias}
                        displayName={getSemanticModelDatasetDisplayName(dataset)}
                        description={getSemanticModelDatasetDescription(dataset)}
                    />
                }
                placement="bottom"
            >
                {children}
            </Tooltip>
        );
    }, []);

    if (!datasets.length) {
        return (
            <LargeModule {...props} dataTestId="semantic-model-datasets-module">
                <EmptyContent
                    icon={Database}
                    title={t('semanticModelDatasets.emptyTitle')}
                    description={t('semanticModelDatasets.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} dataTestId="semantic-model-datasets-module">
            {datasets.map((dataset) => (
                <EntityItem
                    key={dataset.urn}
                    entity={withSemanticModelAlias(dataset)}
                    moduleType={DataHubPageModuleType.SemanticModelDatasets}
                    hideSubtitle
                    hideMatches
                    customDetailsRenderer={renderType}
                    customHoverEntityName={(_entity, children) => renderHover(dataset, children)}
                />
            ))}
        </LargeModule>
    );
}
