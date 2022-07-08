import React, { useState } from 'react';
import styled from 'styled-components';
import { Alert, Divider, Input, Select } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { ChartGroup } from './ChartGroup';
import { useGetAnalyticsChartsQuery, useGetMetadataAnalyticsChartsQuery } from '../../../graphql/analytics.generated';
import { useGetHighlightsQuery } from '../../../graphql/highlights.generated';
import { Highlight } from './Highlight';
import { Message } from '../../shared/Message';
import { useListDomainsQuery } from '../../../graphql/domain.generated';
import filterSearchQuery from '../../search/utils/filterSearchQuery';
import { ANTD_GRAY } from '../../entity/shared/constants';

const HighlightGroup = styled.div`
    display: flex;
    align-items: space-between;
    justify-content: center;
    padding-top: 20px;
    margin-bottom: 10px;
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
    const { data: chartData, loading: chartLoading, error: chartError } = useGetAnalyticsChartsQuery();
    const { data: highlightData, loading: highlightLoading, error: highlightError } = useGetHighlightsQuery();
    const {
        loading: domainLoading,
        error: domainError,
        data: domainData,
    } = useListDomainsQuery({
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

    return (
        <>
            <HighlightGroup>
                {highlightLoading && (
                    <Message type="loading" content="Loading Highlights..." style={{ marginTop: '10%' }} />
                )}
                {highlightError && (
                    <Alert type="error" message={highlightError?.message || 'Highlights failed to load'} />
                )}
                {highlightData?.getHighlights?.map((highlight) => (
                    <Highlight highlight={highlight} shortenValue />
                ))}
            </HighlightGroup>
            <>
                {chartLoading && <Message type="loading" content="Loading Charts..." style={{ marginTop: '10%' }} />}
                {chartError && (
                    <Alert type="error" message={metadataAnalyticsError?.message || 'Charts failed to load'} />
                )}
                {chartData?.getAnalyticsCharts
                    ?.filter((chartGroup) => chartGroup.groupId === 'GlobalMetadataAnalytics')
                    .map((chartGroup) => (
                        <ChartGroup chartGroup={chartGroup} />
                    ))}
            </>
            <>
                {domainLoading && <Message type="loading" content="Loading Domains..." style={{ marginTop: '10%' }} />}
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
                                    option?.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                                }
                            >
                                <Select.Option value="ALL">All</Select.Option>
                                {domainData?.listDomains?.domains.map((domainChoice) => (
                                    <Select.Option value={domainChoice.urn}>
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
                {metadataAnalyticsLoading && (
                    <Message type="loading" content="Loading Charts..." style={{ marginTop: '10%' }} />
                )}
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
                          <ChartGroup chartGroup={chartGroup} />
                      ))}
            </>
            <>
                {chartLoading && <Message type="loading" content="Loading Charts..." style={{ marginTop: '10%' }} />}
                {chartError && <Alert type="error" message={chartError?.message || 'Charts failed to load'} />}
                {!chartLoading &&
                    chartData?.getAnalyticsCharts
                        ?.filter((chartGroup) => chartGroup.groupId === 'DataHubUsageAnalytics')
                        .map((chartGroup) => (
                            <>
                                <Divider />
                                <ChartGroup chartGroup={chartGroup} />
                            </>
                        ))}
            </>
        </>
    );
};
