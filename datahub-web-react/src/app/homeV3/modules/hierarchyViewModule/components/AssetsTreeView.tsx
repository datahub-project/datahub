import React from 'react';

import DomainsTreeView from '@app/homeV3/modules/hierarchyViewModule/components/domains/DomainsTreeView';
import GlossaryTreeView from '@app/homeV3/modules/hierarchyViewModule/components/glossary/GlossaryTreeView';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import { AssetType } from '@app/homeV3/modules/hierarchyViewModule/types';

import { AndFilterInput } from '@types';

interface Props {
    assetType: AssetType;
    assetUrns: string[];
    shouldShowRelatedEntities: boolean;
    relatedEntitiesOrFilters: AndFilterInput[] | undefined;
}

export default function AssetsTreeView({
    assetType,
    assetUrns,
    shouldShowRelatedEntities,
    relatedEntitiesOrFilters,
}: Props) {
    if (assetType === ASSET_TYPE_DOMAINS) {
        return (
            <DomainsTreeView
                assetUrns={assetUrns}
                shouldShowRelatedEntities={shouldShowRelatedEntities}
                relatedEntitiesOrFilters={relatedEntitiesOrFilters}
            />
        );
    }

    if (assetType === ASSET_TYPE_GLOSSARY) {
        return (
            <GlossaryTreeView
                assetUrns={assetUrns}
                shouldShowRelatedEntities={shouldShowRelatedEntities}
                relatedEntitiesOrFilters={relatedEntitiesOrFilters}
            />
        );
    }

    return null;
}
