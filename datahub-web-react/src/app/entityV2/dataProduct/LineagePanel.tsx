import { Button } from '@components';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { StyledHeaderWrapper } from '@app/entityV2/dataProduct/AssetsSections';
import {
    getDirectDownstreamSummary,
    getDirectUpstreamSummary,
} from '@app/entityV2/shared/containers/profile/sidebar/Lineage/utils';
import { SummaryTabHeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { useAppConfig } from '@app/useAppConfig';

import { useGetSearchAcrossLineageCountsQuery } from '@graphql/lineage.generated';

const LineageWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 100px;
`;

const CountsText = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 14px;
    font-weight: 500;
`;

export const LineagePanel = () => {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const routeToTab = useRouteToTab();
    const { urn } = useEntityData();
    const { config } = useAppConfig();
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const lineageEnabled = config.featureFlags.dataProductLineageEnabled;

    const { data, loading } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: !lineageEnabled,
    });

    if (!lineageEnabled) {
        return null;
    }

    const upstreamCount = data?.upstreams ? getDirectUpstreamSummary(data.upstreams as any)?.total || 0 : 0;
    const downstreamCount = data?.downstreams ? getDirectDownstreamSummary(data.downstreams as any)?.total || 0 : 0;

    if (!loading && upstreamCount === 0 && downstreamCount === 0) {
        return null;
    }

    return (
        <LineageWrapper>
            <StyledHeaderWrapper>
                <SummaryTabHeaderTitle
                    icon={<TreeStructure size={16} color={theme.colors.textSecondary} />}
                    title={t('tab.lineage')}
                />
                <Button variant="link" onClick={() => routeToTab({ tabName: t('tab.lineage') })}>
                    {tc('viewAll')}
                </Button>
            </StyledHeaderWrapper>
            {!loading && (
                <CountsText>
                    {t('dataProduct.lineageCounts', { upstream: upstreamCount, downstream: downstreamCount })}
                </CountsText>
            )}
        </LineageWrapper>
    );
};
