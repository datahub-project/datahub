import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import { ArrowRightOutlined } from '@ant-design/icons';
import {
    AssertionResultType,
    AssertionRunEventsResult,
    AssertionRunStatus,
    DataPlatform,
    EntityType,
} from '../../../../../../types.generated';
import { getResultErrorMessage, getResultIcon, getResultText } from './assertionUtils';
import { AssertionResultTimeline, TimeRange } from './AssertionResultTimeline';
import { DatasetAssertionResultDetails } from './DatasetAssertionResultDetails';
import { LinkWrapper } from '../../../../../shared/LinkWrapper';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

const RESULT_CHART_WIDTH_PX = 800;

const AssertionResultIcon = styled.span`
    margin-right: 8px;
`;

const AssertionResultDetailsContainer = styled.div`
    margin-bottom: 4px;
`;

const AssertionResultErrorMessage = styled.div`
    max-width: 250px;
    margin-bottom: 4px;
`;

const AssertionResultInitializingMessage = styled.div`
    max-width: 250px;
    margin-bottom: 4px;
`;

type Props = {
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    platform?: DataPlatform | null;
};

export const AcrylAssertionResultsChartTimeline = ({ results, platform, timeRange }: Props) => {
    const entityRegistry = useEntityRegistry();
    const completedRuns =
        results?.runEvents?.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

    /**
     * Data for the timeline of assertion results.
     */
    const timelineData =
        completedRuns
            .filter((runEvent) => !!runEvent.result)
            .map((runEvent) => {
                const { result } = runEvent;

                if (!result) throw new Error('Completed assertion run event does not have a result.');

                const resultTime = new Date(runEvent.timestampMillis);
                const localTime = resultTime.toLocaleString();
                const gmtTime = resultTime.toUTCString();
                const resultUrl = result.externalUrl;
                const isInitializing = result.type === AssertionResultType.Init;
                const errorMessage = getResultErrorMessage(result);
                const platformName =
                    (platform && entityRegistry.getDisplayName(EntityType.DataPlatform, platform)) || undefined;

                /**
                 * Create a "result" to render in the timeline chart.
                 */
                return {
                    time: runEvent.timestampMillis,
                    result: {
                        type: result.type,
                        resultUrl,
                        title: (
                            <>
                                <AssertionResultIcon>{getResultIcon(result.type)}</AssertionResultIcon>
                                <Typography.Text strong>{getResultText(result.type)}</Typography.Text>
                            </>
                        ),
                        content: (
                            <>
                                <AssertionResultDetailsContainer>
                                    <DatasetAssertionResultDetails result={result} />
                                </AssertionResultDetailsContainer>
                                {isInitializing && (
                                    <AssertionResultInitializingMessage>
                                        Collecting the information required to evaluate this assertion.
                                    </AssertionResultInitializingMessage>
                                )}
                                {errorMessage && (
                                    <AssertionResultErrorMessage>{errorMessage}</AssertionResultErrorMessage>
                                )}
                                <div>
                                    <Tooltip title={`${gmtTime}`}>
                                        <Typography.Text type="secondary">{localTime}</Typography.Text>
                                    </Tooltip>
                                </div>
                                {resultUrl && (
                                    <LinkWrapper to={resultUrl} target="_blank">
                                        {platformName ? `View in ${platformName}` : 'View results'}{' '}
                                        <ArrowRightOutlined />
                                    </LinkWrapper>
                                )}
                            </>
                        ),
                    },
                };
            }) || [];

    return <AssertionResultTimeline width={RESULT_CHART_WIDTH_PX} data={timelineData} timeRange={timeRange} />;
};
