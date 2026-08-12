import { Button } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { SummaryModuleContent, SummaryModuleHeader } from '@app/entityV2/dataProduct/SummaryModule.components';
import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import { OUTPUT_PORTS_FIELD } from '@app/search/utils/constants';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';
import { Entity } from '@types';

const COUNT = 5;

const ViewAllButton = styled(Button)`
    margin: 0 16px 0 auto;
    padding-right: 8px;
`;

export const CompactOutputPortsSection = () => {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const routeToTab = useRouteToTab();
    const { urn } = useEntityData();

    const { data, loading } = useListDataProductAssetsQuery({
        variables: {
            urn,
            input: {
                query: '*',
                start: 0,
                count: COUNT,
                filters: [{ field: OUTPUT_PORTS_FIELD, values: ['true'] }],
            },
        },
    });

    const numResults = data?.listDataProductAssets?.total || 0;
    const results = data?.listDataProductAssets?.searchResults || [];

    if (!data || !results.length) return null;

    return (
        <ModuleContainer
            $height="316px"
            data-testid="data-product-output-ports-module"
            style={{ flex: 1, minWidth: 240 }}
        >
            <SummaryModuleHeader>
                <ModuleName text={t('dataProduct.outputPortsCountTitle', { count: numResults })} />
            </SummaryModuleHeader>
            <SummaryModuleContent $hasFooter>
                {!loading &&
                    results.map((searchResult) => {
                        const { entity } = searchResult;
                        return (
                            <AutoCompleteEntityItem
                                key={entity.urn}
                                entity={entity as Entity}
                                hideMatches
                                dataTestId={`output-port-${entity.urn}`}
                            />
                        );
                    })}
            </SummaryModuleContent>
            <ViewAllButton
                variant="link"
                color="gray"
                size="sm"
                onClick={() => routeToTab({ tabName: t('tab.outputPorts') })}
            >
                {tc('viewAll')}
            </ViewAllButton>
        </ModuleContainer>
    );
};
