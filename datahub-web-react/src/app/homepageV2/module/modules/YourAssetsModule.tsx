import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useGetAssetsYouOwn } from '@app/homeV2/reference/sections/assets/useGetAssetsYouOwn';
import EmptyContent from '@app/homepageV2/module/components/EmptyContent';
import EntityItem from '@app/homepageV2/module/components/EntityItem';
import LargeModule from '@app/homepageV2/module/components/LargeModule';
import { ModuleProps } from '@app/homepageV2/module/types';

export default function YourAssetsModule(props: ModuleProps) {
    const { user } = useUserContext();
    const { originEntities, loading } = useGetAssetsYouOwn(user);

    return (
        <LargeModule {...props} loading={loading}>
            {originEntities.length === 0 ? (
                <EmptyContent
                    icon="User"
                    title="No Owned Assets"
                    description="Select an asset and add yourself as an owner to see the assets in this list"
                    linkText="Discover assets to subscribe to"
                    onLinkClick={() => {}}
                />
            ) : (
                originEntities.map((entity) => <EntityItem entity={entity} key={entity.urn} />)
            )}
        </LargeModule>
    );
}
