import React from 'react';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { DataHubPageModuleType, Entity } from '@types';

const AssetCollectionModule = (props: ModuleProps) => {
    const assetUrns =
        props.module.properties.params.assetCollectionParams?.assetUrns.filter(
            (urn): urn is string => typeof urn === 'string',
        ) || [];

    const { entities, loading } = useGetEntities(assetUrns);

    return (
        <LargeModule {...props} loading={loading}>
            {entities?.length === 0 ? (
                <EmptyContent
                    icon="Stack"
                    title="No Assets"
                    description="Edit the module and add assets to see them in this list"
                />
            ) : (
                entities
                    .filter((entity): entity is Entity => entity !== null)
                    .map((entity) => (
                        <EntityItem
                            entity={entity}
                            key={entity?.urn}
                            moduleType={DataHubPageModuleType.AssetCollection}
                        />
                    ))
            )}
        </LargeModule>
    );
};

export default AssetCollectionModule;
