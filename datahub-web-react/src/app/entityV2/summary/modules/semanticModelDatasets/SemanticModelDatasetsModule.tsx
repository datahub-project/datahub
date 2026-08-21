import { Text } from '@components';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useSemanticModelMemberDatasets } from '@app/entityV2/summary/modules/semanticModelDatasets/useSemanticModelMemberDatasets';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType } from '@types';

export default function SemanticModelDatasetsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const entityRegistry = useEntityRegistryV2();
    const { datasets, loading } = useSemanticModelMemberDatasets();

    if (!loading && !datasets.length) {
        return (
            <LargeModule {...props} loading={loading} dataTestId="semantic-model-datasets-module">
                <EmptyContent
                    icon={Database}
                    title={t('semanticModelDatasets.emptyTitle')}
                    description={t('semanticModelDatasets.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} loading={loading} dataTestId="semantic-model-datasets-module">
            {datasets.map((dataset) => {
                const typeName = entityRegistry.getEntityName(dataset.type) ?? dataset.type;
                return (
                    <EntityItem
                        key={dataset.urn}
                        entity={dataset}
                        moduleType={DataHubPageModuleType.SemanticModelDatasets}
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
