import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import generateUseListDataProductAssets from '@app/entityV2/dataProduct/generateUseListDataProductAssets';
import { generateUseListDataProductAssetsCount } from '@app/entityV2/dataProduct/generateUseListDataProductAssetsCount';
import { SearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { OUTPUT_PORTS_FIELD } from '@app/search/utils/constants';

const ToggleHeader = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px 20px 0;
`;

const ToggleOption = styled.button<{ $active?: boolean }>`
    background: ${(props) => (props.$active ? props.theme.colors.bgSurfaceBrand : 'transparent')};
    border: 1px solid ${(props) => (props.$active ? props.theme.colors.borderBrand : props.theme.colors.border)};
    border-radius: 16px;
    color: ${(props) => (props.$active ? props.theme.colors.textBrand : props.theme.colors.textSecondary)};
    cursor: pointer;
    font-size: 13px;
    font-weight: 500;
    padding: 4px 12px;

    &:hover {
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
`;

const OUTPUT_PORT_FILTER = [{ field: OUTPUT_PORTS_FIELD, values: ['true'] }];

export function OutputPortsTab() {
    const { t } = useTranslation('entity.types');
    const { urn, entityData } = useEntityData();
    const routeToTab = useRouteToTab();

    const useOutputPortsCount = generateUseListDataProductAssetsCount({
        urn,
        extraFilters: OUTPUT_PORT_FILTER,
    });
    const { total: outputPortsCount = 0 } = useOutputPortsCount({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 0,
                filters: [],
            },
        },
    });

    const assetsCount = entityData?.entityCount ?? 0;

    return (
        <>
            <ToggleHeader>
                <ToggleOption $active type="button">
                    {t('dataProduct.outputPortsToggle', { count: outputPortsCount })}
                </ToggleOption>
                <ToggleOption type="button" onClick={() => routeToTab({ tabName: t('tab.assets') })}>
                    {t('dataProduct.allAssetsToggle', { count: assetsCount })}
                </ToggleOption>
            </ToggleHeader>
            <SearchCardContext.Provider value={{ showRemovalFromList: true }}>
                <EmbeddedListSearchSection
                    useGetSearchResults={generateUseListDataProductAssets({ urn, extraFilters: OUTPUT_PORT_FILTER })}
                    useGetSearchCountResult={generateUseListDataProductAssetsCount({
                        urn,
                        extraFilters: OUTPUT_PORT_FILTER,
                    })}
                    emptySearchQuery="*"
                    placeholderText={t('shared.filterAssetsPlaceholder')}
                    skipCache
                    applyView
                />
            </SearchCardContext.Provider>
        </>
    );
}
