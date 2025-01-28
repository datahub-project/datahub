import React, { useState } from 'react';
import styled from 'styled-components';
import { Alert, Divider, Input, Select } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { ChartGroup } from './ChartGroup';
import { useGetAnalyticsChartsQuery, useGetMetadataAnalyticsChartsQuery } from '../../../graphql/analytics.generated';
import { useGetHighlightsQuery } from '../../../graphql/highlights.generated';
import { Highlight } from './Highlight';
import { Message } from '../../shared/Message';
import { useListDomainsQuery } from '../../../graphql/domain.generated';
import filterSearchQuery from '../../search/utils/filterSearchQuery';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useUserContext } from '../../context/useUserContext';
import { useIsThemeV2 } from '../../useIsThemeV2';

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
`;

const HighlightGroup = styled.div`
    margin-top: 20px;
    padding: 0 20px;
    margin-bottom: 10px;
    display: grid;
    grid-template-rows: auto auto;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
`;

const MetadataAnalyticsInput = styled.div`
    display: flex;
`;

const MetadataAnalyticsPlaceholder = styled.span`
    margin: 25px;
    margin-bottom: 50px;
    font-size: 18px;
    color: ${ANTD_GRAY[7]};
`;

const DomainSelect = styled(Select)`
    margin-left: 25px;
    width: 200px;
    display: inline-block;
`;

const StyledSearchBar = styled(Input)`
    &&& {
        margin-left: 10px;
        border-radius: 70px;
        color: ${ANTD_GRAY[7]};
        width: 250px;
    }
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
    const [domain, setDomain] = useState('');
    const [stagedQuery, setStagedQuery] = useState('');
    const [query, setQuery] = useState('');

    const onDomainChange = (inputDomain) => setDomain(inputDomain);
    const onStagedQueryChange = (inputQuery) => setStagedQuery(inputQuery);
    const {
        loading: metadataAnalyticsLoading,
        error: metadataAnalyticsError,
        data: metadataAnalyticsData,
    } = useGetMetadataAnalyticsChartsQuery({
        variables: {
            input: {
                entityType: null,
                domain,
                query,
            },
        },
        skip: domain === '' && query === '',
    });

    const isLoading = highlightLoading || chartLoading || domainLoading || metadataAnalyticsLoading;
    return (
        <PageContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            {isLoading && <Message type="loading" content="Loading…" style={{ marginTop: '10%' }} />}
            <HighlightGroup>
                {highlightError && (
                    <Alert type="error" message={highlightError?.message || 'Highlights failed to load'} />
                )}
                {highlightData?.getHighlights?.map((highlight) => (
                    <Highlight highlight={highlight} shortenValue key={highlight.title} />
                ))}
            </HighlightGroup>
            <>
                {chartError && (
                    <Alert type="error" message={metadataAnalyticsError?.message || 'Charts failed to load'} />
                )}
                {chartData?.getAnalyticsCharts
                    ?.filter((chartGroup) => chartGroup.groupId === 'GlobalMetadataAnalytics')
                    .map((chartGroup) => (
                        <ChartGroup chartGroup={chartGroup} key={chartGroup.title} />
                    ))}
            </>
            <>
                {domainError && (
                    <Alert type="error" message={metadataAnalyticsError?.message || 'Domains failed to load'} />
                )}
                {!chartLoading && (
                    <>
                        <Divider />
                        <MetadataAnalyticsInput>
                            <DomainSelect
                                showSearch
                                placeholder="Select a domain"
                                onChange={onDomainChange}
                                filterOption={(input, option) =>
                                    option?.children?.toLowerCase()?.indexOf(input.toLowerCase()) >= 0
                                }
                            >
                                <Select.Option value="ALL">All</Select.Option>
                                {domainData?.listDomains?.domains?.map((domainChoice) => (
                                    <Select.Option value={domainChoice.urn} key={domainChoice.urn}>
                                        {domainChoice?.properties?.name}
                                    </Select.Option>
                                ))}
                            </DomainSelect>
                            <StyledSearchBar
                                placeholder="Search"
                                onPressEnter={(e) => {
                                    e.stopPropagation();
                                    setQuery(filterSearchQuery(stagedQuery || ''));
                                }}
                                value={stagedQuery}
                                onChange={(e) => onStagedQueryChange(e.target.value)}
                                data-testid="analytics-search-input"
                                prefix={
                                    <SearchOutlined onClick={() => setQuery(filterSearchQuery(stagedQuery || ''))} />
                                }
                            />
                        </MetadataAnalyticsInput>
                    </>
                )}
            </>
            <>
                {metadataAnalyticsError && (
                    <Alert type="error" message={metadataAnalyticsError?.message || 'Charts failed to load'} />
                )}
                {domain === '' && query === ''
                    ? !chartLoading && (
                          <MetadataAnalyticsPlaceholder>
                              Please specify domain or query to get granular results
                          </MetadataAnalyticsPlaceholder>
                      )
                    : metadataAnalyticsData?.getMetadataAnalyticsCharts?.map((chartGroup) => (
                          <ChartGroup chartGroup={chartGroup} key={chartGroup.title} />
                      ))}
            </>
            <>
                {chartError && <Alert type="error" message={chartError?.message || 'Charts failed to load'} />}
                {!chartLoading &&
                    chartData?.getAnalyticsCharts
                        ?.filter((chartGroup) => chartGroup.groupId === 'DataHubUsageAnalytics')
                        .map((chartGroup) => (
                            <React.Fragment key={chartGroup.title}>
                                <Divider />
                                <ChartGroup chartGroup={chartGroup} />
                            </React.Fragment>
                        ))}
            </>
        </PageContainer>
    );
};
