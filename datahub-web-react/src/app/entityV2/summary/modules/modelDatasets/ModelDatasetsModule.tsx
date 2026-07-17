import { Text } from '@components';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Entity, ModelDataset } from '@types';

type EntityDataWithDatasets = {
    info?: {
        datasets?: ModelDataset[] | null;
    } | null;
};

export default function ModelDatasetsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();

    const datasets = (entityData as EntityDataWithDatasets)?.info?.datasets ?? [];

    if (!datasets.length) {
        return (
            <LargeModule {...props} dataTestId="model-datasets-module">
                <EmptyContent
                    icon={Database}
                    title={t('modelDatasets.emptyTitle')}
                    description={t('modelDatasets.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} dataTestId="model-datasets-module">
            {datasets.map((dataset) => {
                const source = dataset.source as Entity;
                const typeName = entityRegistry.getEntityName(source.type) ?? source.type;
                return (
                    <EntityItem
                        key={source.urn}
                        entity={source}
                        moduleType={DataHubPageModuleType.ModelDatasets}
                        customDetailsRenderer={() => (
                            <Text size="sm" color="gray">
                                {typeName}
                            </Text>
                        )}
                    />
                );
            })}
        </LargeModule>
    );
}
