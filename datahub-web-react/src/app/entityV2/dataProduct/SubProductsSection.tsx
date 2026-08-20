import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SummaryModuleContent, SummaryModuleHeader } from '@app/entityV2/dataProduct/SummaryModule.components';
import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';

import { Entity, ScrollResults } from '@types';

type EntityDataWithChildren = {
    childDataProducts?: ScrollResults | null;
};

const ShowingSubtitle = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 400;
`;

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
                <ModuleName text={t('dataProduct.subProductsCountTitle', { count: children.length })} />
                {total > children.length && (
                    <ShowingSubtitle>
                        {t('dataProduct.showingSubProductsOfTotal', { count: children.length, total })}
                    </ShowingSubtitle>
                )}
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
