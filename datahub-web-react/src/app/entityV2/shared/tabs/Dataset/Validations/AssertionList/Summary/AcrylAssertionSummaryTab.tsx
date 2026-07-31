import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';
import { Card, Empty } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { AcrylAssertionSummaryCard } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Summary/AcrylAssertionSummaryCard';
import { AssertionGroup, AssertionStatusSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import {
    getAssertionGroupName,
    getAssertionGroupTypeIcon,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useGetValidationsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/useGetValidationsTab';
import {
    ASSERTION_CUSTOM_TYPE_FILTER_NAME,
    ASSERTION_STATUS_FILTER_NAME,
    ASSERTION_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { Button } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useSearchAssertionsQuery } from '@src/graphql/assertion.generated';

import { AssertionType, EntityType, FacetFilterInput, FilterOperator } from '@types';

const LoadingBody = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    padding: 24px;
    row-gap: 24px;
    column-gap: 24px;
    overflow: auto;
`;

const CardSkeleton = styled(Card)`
    && {
        padding: 0px 12px 12px 0px;
        height: 210px;
        border-radius: 8px;
        width: 100%;
    }
`;

const AcrylAssertionSummaryContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    padding: 16px;
    row-gap: 24px;
    column-gap: 24px;
    overflow: auto;
`;

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const getBaseFilter: (urns: string[]) => FacetFilterInput = (urns: string[]) => ({
    field: LEGACY_ENTITY_FILTER_NAME,
    values: urns,
    condition: FilterOperator.Equal,
});
const getDefaultQueryVariables = (filters: FacetFilterInput[]) => ({
    input: {
        types: [EntityType.Assertion],
        query: '*',
        start: 0,
        count: 1, // We only really need the facets
        orFilters: [
            {
                and: filters,
            },
        ],
    },
});

const AcrylAssertionTypeSummary = ({
    assertionType,
    customType,
    urnsToSearch,
}: {
    assertionType: AssertionType;
    customType?: string;
    urnsToSearch: string[];
}) => {
    const filters: FacetFilterInput[] = [
        getBaseFilter(urnsToSearch),
        {
            field: ASSERTION_TYPE_FILTER_NAME,
            values: [assertionType],
        },
    ];

    if (customType) {
        filters.push({
            field: ASSERTION_CUSTOM_TYPE_FILTER_NAME,
            values: [customType],
        });
    }

    const variables = getDefaultQueryVariables(filters);
    const { data, loading } = useSearchAssertionsQuery({
        variables,
        fetchPolicy: 'cache-first',
    });

    if (loading) {
        return <CardSkeleton loading />;
    }

    const statusFacet = data?.searchAcrossEntities?.facets?.find(
        (facet) => facet.field === ASSERTION_STATUS_FILTER_NAME,
    );
    const passingCount = statusFacet?.aggregations?.find((aggregation) => aggregation.value === 'PASSING')?.count || 0;
    const failingCount = statusFacet?.aggregations?.find((aggregation) => aggregation.value === 'FAILING')?.count || 0;
    const erroringCount = statusFacet?.aggregations?.find((aggregation) => aggregation.value === 'ERROR')?.count || 0;
    const initializingCount =
        statusFacet?.aggregations?.find((aggregation) => aggregation.value === 'INIT')?.count || 0;
    const totalCount = data?.searchAcrossEntities?.total ?? 0;
    const totalAssertionsCount = data?.searchAcrossEntities?.total ?? 0;

    const summary: AssertionStatusSummary = {
        passing: passingCount,
        failing: failingCount,
        erroring: erroringCount,
        initializing: initializingCount,
        notRunning: totalCount - passingCount - failingCount - erroringCount - initializingCount,
        total: totalCount,
        totalAssertions: totalAssertionsCount,
    };

    const displayType = customType || assertionType;
    const assertionGroup: AssertionGroup = {
        name: getAssertionGroupName(displayType),
        icon: getAssertionGroupTypeIcon(displayType),
        assertions: [],
        summary,
        type: assertionType,
    };

    return <AcrylAssertionSummaryCard group={assertionGroup} />;
};

export const AcrylAssertionSummaryTab = () => {
    const { t } = useTranslation('entity.profile.validations');
    const { urn: entityUrn, entityData, loading: entityLoading } = useEntityData();
    const history = useHistory();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const { pathname } = useLocation();
    const { basePath } = useGetValidationsTab(pathname, ['Summary']);
    const siblingUrns = entityData?.siblingsSearch?.searchResults?.map((result) => result.entity.urn);
    // Include siblings if not in hide sibling mode
    const urnsToSearch = !isHideSiblingMode ? [entityUrn, ...(siblingUrns || [])] : [entityUrn];
    const filters: FacetFilterInput[] = [getBaseFilter(urnsToSearch)];
    const variables = getDefaultQueryVariables(filters);
    const { data: assertionData, loading: assertionDataLoading } = useSearchAssertionsQuery({
        variables,
        fetchPolicy: 'cache-first',
    });

    if (entityLoading || assertionDataLoading) {
        return (
            <LoadingBody>
                <CardSkeleton loading />
                <CardSkeleton loading />
                <CardSkeleton loading />
                <CardSkeleton loading />
            </LoadingBody>
        );
    }

    const assertionCount = assertionData?.searchAcrossEntities?.total || 0;
    const assertionTypeFacets = assertionData?.searchAcrossEntities?.facets?.find(
        (facet) => facet.field === ASSERTION_TYPE_FILTER_NAME,
    )?.aggregations;
    const customTypeFacets = assertionData?.searchAcrossEntities?.facets?.find(
        (facet) => facet.field === ASSERTION_CUSTOM_TYPE_FILTER_NAME,
    )?.aggregations;

    if (!assertionData || assertionCount === 0 || !assertionTypeFacets) {
        return (
            <EmptyContainer>
                <Empty description={t('assertionSummary.noAssertionsCreated')} image={Empty.PRESENTED_IMAGE_SIMPLE} />
                <Button
                    onClick={() =>
                        history.replace(`${basePath}/List?${SEPARATE_SIBLINGS_URL_PARAM}=${isHideSiblingMode}`)
                    }
                >
                    {t('assertionSummary.goToAssertions')}
                    <ArrowRight />
                </Button>
            </EmptyContainer>
        );
    }

    const summaryItems: Array<{ assertionType: AssertionType; customType?: string }> = [];
    assertionTypeFacets.forEach((facet) => {
        if (facet.value === AssertionType.Custom && customTypeFacets?.length) {
            customTypeFacets.forEach((ct) => {
                summaryItems.push({ assertionType: AssertionType.Custom, customType: ct.value });
            });
        } else {
            summaryItems.push({ assertionType: facet.value as AssertionType });
        }
    });

    return (
        <AcrylAssertionSummaryContainer>
            {summaryItems.map((item) => (
                <AcrylAssertionTypeSummary
                    key={item.customType ? `${item.assertionType}-${item.customType}` : item.assertionType}
                    assertionType={item.assertionType}
                    customType={item.customType}
                    urnsToSearch={urnsToSearch}
                />
            ))}
        </AcrylAssertionSummaryContainer>
    );
};
