import { Breadcrumb } from '@components';
import React, { useEffect } from 'react';
import { useLocation, useParams } from 'react-router';

import { VerticalDivider } from '@components/components/Breadcrumb/components';

import { EmbeddedChat } from '@app/chat/EmbeddedChat';
import {
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import RunDetailsContent from '@app/ingestV2/runDetails/RunDetailsContent';
import { formatDateTime } from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

import { useGetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';
import { DataHubAiConversationOriginType } from '@types';

export default function IngestionRunDetailsPage() {
    const { urn } = useParams<{ urn: string }>();

    const { state } = useLocation();
    const [fromUrl, setFromUrl] = React.useState<string>();
    const [name, setName] = React.useState<string>();
    const [runTime, setRunTime] = React.useState<number>();
    const { data, loading, error, refetch } = useGetIngestionExecutionRequestQuery({ variables: { urn } });

    const [titlePill, setTitlePill] = React.useState<React.ReactNode>(null);

    useEffect(() => {
        const sourceName = data?.executionRequest?.source?.name;
        const time = data?.executionRequest?.result?.startTimeMs;
        if (sourceName) {
            setName(sourceName);
        }
        if (time) {
            setRunTime(time);
        }
    }, [data]);

    useEffect(() => {
        if (state?.fromUrl) {
            setFromUrl(state.fromUrl);
        }
    }, [state]);

    const breadCrumb = (
        <Breadcrumb
            items={[
                {
                    label: fromUrl === tabUrlMap[TabType.RunHistory] ? 'Run history' : 'Manage Data Sources',
                    href: fromUrl ?? tabUrlMap[TabType.Sources],
                    separator: <VerticalDivider type="vertical" />,
                },
                ...(name
                    ? [
                          {
                              label: name,
                          },
                      ]
                    : []),
                ...(runTime
                    ? [
                          {
                              label: formatDateTime(runTime),
                          },
                      ]
                    : []),
            ]}
        />
    );

    const chatContext = `The user is viewing ingestion run details for execution request with URN: ${urn}. ${
        name ? `The ingestion source name is "${name}". ` : ''
    } This is a troubleshooting context where the user may ask questions about ingestion failures, logs, or execution details.`;

    let suggestedQuestions;

    if (data?.executionRequest?.result?.status === EXECUTION_REQUEST_STATUS_SUCCESS) {
        suggestedQuestions = [
            'Summarize what was ingested from this run',
            'How can I improve performance for this source?',
            'What source should I connect next?',
        ];
    } else if (data?.executionRequest?.result?.status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS) {
        suggestedQuestions = [
            'Summarize what happened',
            'What is the impact?',
            'What should I resolve before the next run?',
        ];
    } else if (data?.executionRequest?.result?.status === EXECUTION_REQUEST_STATUS_FAILURE) {
        suggestedQuestions = [
            'What’s the main error?',
            'What should I check in my configuration?',
            'Walk me through fixing this step-by-step',
        ];
    } else {
        suggestedQuestions = [
            'What’s the current status of this source?',
            'Should I change any settings for best results?',
            'What should I do next?',
        ];
    }

    return (
        <PageLayout
            title="Run Details"
            titlePill={titlePill}
            rightPanelContent={
                <EmbeddedChat
                    context={chatContext}
                    agentName="IngestionTroubleshooter"
                    originType={DataHubAiConversationOriginType.IngestionUi}
                    title="Ask DataHub - Run Details"
                    chatLocation="ingestion_view_results"
                    contentPlaceholder="Ask DataHub about your run details"
                    suggestedQuestions={suggestedQuestions}
                />
            }
            topBreadcrumb={breadCrumb}
        >
            <RunDetailsContent
                urn={urn}
                data={data}
                refetch={refetch}
                loading={loading}
                error={error}
                setTitlePill={setTitlePill}
            />
        </PageLayout>
    );
}
