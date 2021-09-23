import React from 'react';
import styled from 'styled-components';
import { Alert } from 'antd';

import { SearchablePage } from '../../search/SearchablePage';
import { sampleCharts, sampleHighlights } from '../sampleData';
import { ChartGroup } from './ChartGroup';
import { useGetAnalyticsChartsQuery } from '../../../graphql/analytics.generated';
import { useGetHighlightsQuery } from '../../../graphql/highlights.generated';
import { Highlight } from './Highlight';
import { Message } from '../../shared/Message';

const HighlightGroup = styled.div`
    display: flex;
    align-items: space-between;
    justify-content: center;
    padding-top: 20px;
    margin-bottom: -20px;
`;

const IS_DEV = false;

export const AnalyticsPage = () => {
    const { data: chartData, loading: chartLoading, error: chartError } = useGetAnalyticsChartsQuery();
    const { data: highlightData, loading: highlightLoading, error: highlightError } = useGetHighlightsQuery();

    return (
        <SearchablePage>
            <HighlightGroup>
                {highlightLoading && (
                    <Message type="loading" content="Loading Highlights..." style={{ marginTop: '10%' }} />
                )}
                {highlightError && (
                    <Alert type="error" message={highlightError?.message || 'Highlights failed to load'} />
                )}
                {(IS_DEV ? sampleHighlights : highlightData?.getHighlights)?.map((highlight) => (
                    <Highlight highlight={highlight} shortenValue />
                ))}
            </HighlightGroup>
            <>
                {chartLoading && <Message type="loading" content="Loading Charts..." style={{ marginTop: '10%' }} />}
                {chartError && <Alert type="error" message={chartError?.message || 'Charts failed to load'} />}
                {(IS_DEV ? sampleCharts : chartData?.getAnalyticsCharts)?.map((chartGroup) => (
                    <ChartGroup chartGroup={chartGroup} />
                ))}
            </>
        </SearchablePage>
    );
};
