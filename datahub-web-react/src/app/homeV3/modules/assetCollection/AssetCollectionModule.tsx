import React from 'react';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { Entity } from '@types';

const AssetCollectionModule = (props: ModuleProps) => {
    const entities = props.module.properties.params.assetCollectionParams?.assets || [];
    return (
        <LargeModule {...props}>
            {entities?.length === 0 ? (
                <EmptyContent
                    icon="User"
                    title="No Owned Assets"
                    description="Select an asset and add yourself as an owner to see the assets in this list"
                    linkText="Discover assets to subscribe to"
                    onLinkClick={() => {}}
                />
            ) : (
                entities
                    .filter((entity): entity is Entity => entity !== null)
                    .map((entity) => <EntityItem entity={entity} key={entity?.urn} />)
            )}
        </LargeModule>
    );
};

export default AssetCollectionModule;
