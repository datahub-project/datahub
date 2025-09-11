import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useGetAssetsYouSubscribeTo } from '@app/homeV2/reference/sections/subscriptions/useGetAssetsYouSubscribeTo';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import useNavigateFromSubscriptions from '@app/homeV3/modules/subscriptions/useNavigateFromSubscriptions';

export default function SubscriptionsModule(props: ModuleProps) {
    const { user } = useUserContext();
    const { isReloading, onReloadingFinished } = useModuleContext();
    const { originEntities, loading } = useGetAssetsYouSubscribeTo({
        user,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished(),
    });
    const { navigateToSubscriptions, navigateToSearch } = useNavigateFromSubscriptions();

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={navigateToSubscriptions}>
            {originEntities.length === 0 ? (
                <EmptyContent
                    icon="Bell"
                    title="No Active Subscriptions"
                    description="Subscribe to data assets to get notified about important changes and updates"
                    linkText="Discover assets to subscribe to"
                    onLinkClick={navigateToSearch}
                />
            ) : (
                originEntities.map((entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={props.module.properties.type} />
                ))
            )}
        </LargeModule>
    );
}
