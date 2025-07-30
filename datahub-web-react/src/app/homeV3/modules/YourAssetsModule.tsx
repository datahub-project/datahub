import React, { useCallback } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { useGetAssetsYouOwn } from '@app/homeV2/reference/sections/assets/useGetAssetsYouOwn';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import useSearchYourAssets from '@app/homeV3/modules/useSearchYourAssets';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

import { DataHubPageModuleType } from '@types';

export default function YourAssetsModule(props: ModuleProps) {
    const { user } = useUserContext();
    const { originEntities, loading } = useGetAssetsYouOwn(user);
    const searchForYourAssets = useSearchYourAssets();
    const history = useHistory();

    const navigateToSearch = useCallback(() => {
        navigateToSearchUrl({ query: '*', history });
    }, [history]);

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={searchForYourAssets}>
            {originEntities.length === 0 ? (
                <EmptyContent
                    icon="User"
                    title="No Owned Assets"
                    description="Select an asset and add yourself as an owner to see the assets in this list"
                    linkText="Discover the assets you want to own"
                    onLinkClick={navigateToSearch}
                />
            ) : (
                originEntities.map((entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.OwnedAssets} />
                ))
            )}
        </LargeModule>
    );
}
