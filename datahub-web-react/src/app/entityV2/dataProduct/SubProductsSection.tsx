import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SummaryModuleContent, SummaryModuleHeader } from '@app/entityV2/dataProduct/SummaryModule.components';
import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';

import { Entity, ScrollResults } from '@types';

type EntityDataWithChildren = {
    childDataProducts?: ScrollResults | null;
};

export const SubProductsSection = () => {
    const { t } = useTranslation('entity.types');
    const { entityData } = useEntityData();

    const childDataProducts = (entityData as EntityDataWithChildren | null)?.childDataProducts;
    const total = childDataProducts?.total || 0;
    const children =
        childDataProducts?.searchResults
            ?.map((result) => result.entity)
            .filter((entity): entity is Entity => !!entity) || [];

    if (!total || children.length === 0) {
        return null;
    }

    return (
        <ModuleContainer
            $height="316px"
            data-testid="data-product-sub-products-module"
            style={{ flex: 1, minWidth: 240 }}
        >
            <SummaryModuleHeader>
                <ModuleName text={t('dataProduct.subProductsCountTitle', { count: total })} />
            </SummaryModuleHeader>
            <SummaryModuleContent>
                {children.map((entity) => (
                    <AutoCompleteEntityItem
                        key={entity.urn}
                        entity={entity}
                        hideMatches
                        dataTestId={`sub-product-${entity.urn}`}
                    />
                ))}
            </SummaryModuleContent>
        </ModuleContainer>
    );
};
