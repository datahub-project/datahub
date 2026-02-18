import { Breadcrumb } from '@components';
import React, { useEffect, useState } from 'react';
import { useLocation, useParams } from 'react-router';

import { VerticalDivider } from '@components/components/Breadcrumb/components';

import CollapsibleChat from '@app/chat/CollapsibleChat';
import RunDetailsContent from '@app/ingestV2/runDetails/RunDetailsContent';
import { getSuggestedQuestions } from '@app/ingestV2/runDetails/utils';
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

    const [isChatOpen, setIsChatOpen] = useState(true);

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

    const suggestedQuestions = getSuggestedQuestions(data?.executionRequest?.result?.status || '');

    return (
        <PageLayout
            title="Run Details"
            titlePill={titlePill}
            rightPanelContent={
                <CollapsibleChat
                    setIsChatOpen={setIsChatOpen}
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
            isRightPanelCollapsed={!isChatOpen}
        >
            <RunDetailsContent
                urn={urn}
                data={data}
                refetch={refetch}
                loading={loading}
                error={error}
                setTitlePill={setTitlePill}
                isChatOpen={isChatOpen}
                setIsChatOpen={setIsChatOpen}
            />
        </PageLayout>
    );
}
