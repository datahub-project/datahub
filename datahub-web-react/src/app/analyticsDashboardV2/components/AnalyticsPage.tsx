import { Loader, PageTitle } from '@components';
import { Alert, Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ChartGroup } from '@app/analyticsDashboardV2/components/ChartGroup';
import { Highlight } from '@app/analyticsDashboardV2/components/Highlight';
import { useUserContext } from '@app/context/useUserContext';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useGetAnalyticsChartsQuery, useGetMetadataAnalyticsChartsQuery } from '@graphql/analytics.generated';
import { useListDomainsQuery } from '@graphql/domain.generated';
import { useGetHighlightsQuery } from '@graphql/highlights.generated';

const PageContainer = styled.div<{ isV2: boolean; $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.isV2 ? '#fff' : 'inherit')};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 100%;
        margin: 5px;
        overflow: auto;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-right: ${props.isV2 ? '24px' : '0'};
        margin-bottom: ${props.isV2 ? '24px' : '0'};
    `}
    border-radius: ${(props) => {
        if (props.isV2 && props.$isShowNavBarRedesign) return props.theme.styles['border-radius-navbar-redesign'];
        return props.isV2 ? '8px' : '0';
    }};
    padding: 24px;
    padding-bottom: 48px;
`;

const HighlightGroup = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
    gap: 16px;
    margin-bottom: 24px;

    @media (min-width: 1200px) {
        grid-template-columns: repeat(4, 1fr);
    }
`;

const DomainSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 24px 16px;
`;

const TitleContainer = styled.div`
    margin-bottom: 8px;
`;

const FilterSection = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 16px;
`;

const DomainSelect = styled(Select)`
    width: 220px;
`;

const LoaderContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 200px;
`;

const Divider = styled.div`
    height: 1px;
    background: ${(props) => props.theme.styles['border-color-default']};
    margin: 32px 0;
`;

export const AnalyticsPage = () => {
    const isV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const me = useUserContext();
    const canManageDomains = me?.platformPrivileges?.createDomains;
    const { data: chartData, loading: chartLoading, error: chartError } = useGetAnalyticsChartsQuery();
    const { data: highlightData, loading: highlightLoading, error: highlightError } = useGetHighlightsQuery();
    const {
        loading: domainLoading,
        error: domainError,
        data: domainData,
    } = useListDomainsQuery({
        skip: !canManageDomains,
        variables: {
            input: {
                start: 0,
                count: 1000,
            },
        },
        fetchPolicy: 'no-cache',
    });
    const [domain, setDomain] = useState('ALL');

    const onDomainChange = (inputDomain) => setDomain(inputDomain);
    const {
        loading: metadataAnalyticsLoading,
        error: metadataAnalyticsError,
        data: metadataAnalyticsData,
    } = useGetMetadataAnalyticsChartsQuery({
        variables: {
            input: {
                entityType: null,
                domain,
                query: '',
            },
        },
        skip: false,
    });

    const isLoading = highlightLoading || chartLoading || domainLoading || metadataAnalyticsLoading;

    return (
        <PageContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            {isLoading && (
                <LoaderContainer>
                    <Loader />
                </LoaderContainer>
            )}
            {!isLoading && (
                <>
                    <HighlightGroup>
                        {highlightError && (
                            <Alert type="error" message={highlightError?.message || 'Failed to load highlights'} />
                        )}
                        {highlightData?.getHighlights?.map((highlight) => (
                            <Highlight highlight={highlight} shortenValue key={highlight.title} />
                        ))}
                    </HighlightGroup>

                    {chartError && (
                        <Alert type="error" message={metadataAnalyticsError?.message || 'Failed to load charts'} />
                    )}
                    {chartData?.getAnalyticsCharts
                        ?.filter((chartGroup) => chartGroup.groupId === 'GlobalMetadataAnalytics')
                        .map((chartGroup) => (
                            <ChartGroup
                                chartGroup={{ ...chartGroup, title: 'Data Landscape Summary' }}
                                key={chartGroup.title}
                            />
                        ))}

                    <DomainSection>
                        <TitleContainer>
                            <PageTitle title="Domain Landscape Summary" variant="sectionHeader" />
                        </TitleContainer>
                        <FilterSection>
                            {domainError && (
                                <Alert
                                    type="error"
                                    message={metadataAnalyticsError?.message || 'Failed to load domains'}
                                />
                            )}
                            <DomainSelect
                                showSearch
                                placeholder="Select domain"
                                value={domain}
                                onChange={onDomainChange}
                                filterOption={(input, option) =>
                                    option?.children?.toLowerCase()?.indexOf(input.toLowerCase()) >= 0
                                }
                            >
                                <Select.Option value="ALL">All Domains</Select.Option>
                                {domainData?.listDomains?.domains?.map((domainChoice) => (
                                    <Select.Option value={domainChoice.urn} key={domainChoice.urn}>
                                        {domainChoice?.properties?.name}
                                    </Select.Option>
                                ))}
                            </DomainSelect>
                        </FilterSection>
                    </DomainSection>

                    {metadataAnalyticsError && (
                        <Alert type="error" message={metadataAnalyticsError?.message || 'Failed to load charts'} />
                    )}
                    {metadataAnalyticsData?.getMetadataAnalyticsCharts?.map((chartGroup) => (
                        <ChartGroup chartGroup={{ ...chartGroup, title: '' }} key={chartGroup.groupId} />
                    ))}

                    {chartError && <Alert type="error" message={chartError?.message || 'Failed to load charts'} />}
                    {chartData?.getAnalyticsCharts
                        ?.filter((chartGroup) => chartGroup.groupId === 'DataHubUsageAnalytics')
                        .map((chartGroup) => (
                            <React.Fragment key={chartGroup.title}>
                                <Divider />
                                <ChartGroup chartGroup={{ ...chartGroup, title: 'Usage Analytics' }} />
                            </React.Fragment>
                        ))}
                </>
            )}
        </PageContainer>
    );
};
