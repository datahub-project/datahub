import { LoadingOutlined } from '@ant-design/icons';
import { ApolloError } from '@apollo/client';
import { Icon, Pill } from '@components';
import { message } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { Tab, Tabs } from '@components/components/Tabs/Tabs';

import analytics, { EventType } from '@app/analytics';
import { getIngestionSourceStatus } from '@app/ingest/source/utils';
import { LogsTab } from '@app/ingestV2/executions/components/LogsTab';
import { RecipeTab } from '@app/ingestV2/executions/components/RecipeTab';
import { SummaryTab } from '@app/ingestV2/executions/components/SummaryTab';
import { EXECUTION_REQUEST_STATUS_LOADING, EXECUTION_REQUEST_STATUS_RUNNING } from '@app/ingestV2/executions/constants';
import { TabType } from '@app/ingestV2/executions/types';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingestV2/executions/utils';
import { Message } from '@app/shared/Message';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';
import { ExecutionRequestResult } from '@types';

const ContentWrapper = styled.div`
    padding: 0 0 16px 20px;
    overflow: auto;
    height: 100%;
`;

interface Props {
    urn: string;
    data: GetIngestionExecutionRequestQuery | undefined;
    loading?: boolean;
    error?: ApolloError;
    refetch: () => void;
    setTitlePill?: (pill: React.ReactNode) => void;
}

export default function RunDetailsContent({ urn, data, loading, error, refetch, setTitlePill }: Props) {
    const location = useLocation();
    const result = data?.executionRequest?.result as Partial<ExecutionRequestResult>;
    const status = getIngestionSourceStatus(result);

    const [selectedTab, setSelectedTab] = useState<TabType>(TabType.Summary);

    const sendAnalyticsTabViewedEvent = useCallback(
        (tab: TabType) => {
            if (!result) return;
            analytics.event({
                type: EventType.IngestionExecutionResultViewedEvent,
                executionUrn: urn,
                executionStatus: status || 'UNKNOWN',
                viewedSection: tab,
                sourceType: data?.executionRequest?.source?.type,
            });
        },
        [result, urn, status, data?.executionRequest?.source?.type],
    );

    const selectTab = (tab: TabType) => {
        setSelectedTab(tab);
        sendAnalyticsTabViewedEvent(tab);
    };

    useEffect(() => {
        sendAnalyticsTabViewedEvent(TabType.Summary);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const ResultIcon = status && getExecutionRequestStatusIcon(status);
    const resultColor = status ? getExecutionRequestStatusDisplayColor(status) : 'gray';
    const titlePill = useMemo(
        () =>
            status &&
            ResultIcon && (
                <Pill
                    customIconRenderer={() =>
                        status === EXECUTION_REQUEST_STATUS_LOADING || status === EXECUTION_REQUEST_STATUS_RUNNING ? (
                            <LoadingOutlined />
                        ) : (
                            <Icon icon={ResultIcon} source="phosphor" size="lg" />
                        )
                    }
                    label={getExecutionRequestStatusDisplayText(status)}
                    color={resultColor}
                    size="md"
                />
            ),
        [ResultIcon, resultColor, status],
    );

    useEffect(() => {
        if (setTitlePill) {
            setTitlePill(titlePill);
        }
    }, [setTitlePill, titlePill]);

    useEffect(() => {
        const interval = setInterval(() => {
            if (status === EXECUTION_REQUEST_STATUS_RUNNING) refetch();
        }, 2000);
        return () => clearInterval(interval);
    }, [status, refetch]);
    const tabs: Tab[] = useMemo(
        () => [
            {
                component: (
                    <SummaryTab
                        urn={urn}
                        status={status}
                        result={result}
                        data={data}
                        onTabChange={(tab: TabType) => setSelectedTab(tab)}
                    />
                ),
                key: TabType.Summary,
                name: TabType.Summary,
                dataTestId: 'run-details-summary-tab',
            },
            {
                component: <LogsTab urn={urn} data={data} />,
                key: TabType.Logs,
                name: TabType.Logs,
                dataTestId: 'run-details-logs-tab',
            },
            {
                component: <RecipeTab urn={urn} data={data} />,
                key: TabType.Recipe,
                name: TabType.Recipe,
                dataTestId: 'run-details-recipe-tab',
            },
        ],
        [data, urn, result, status],
    );
    return (
        <ContentWrapper>
            {!data && loading && <Message type="loading" content="Loading execution run details..." />}
            {error && message.error('Failed to load execution run details :(')}
            <Tabs
                tabs={tabs}
                selectedTab={selectedTab}
                onChange={(tab) => selectTab(tab as TabType)}
                getCurrentUrl={() => location.pathname}
                scrollToTopOnChange
                maxHeight="80vh"
                stickyHeader
            />
        </ContentWrapper>
    );
}
