import { Breadcrumb } from '@components';
import React, { useEffect } from 'react';
import { useLocation, useParams } from 'react-router';

import { VerticalDivider } from '@components/components/Breadcrumb/components';

import RunDetailsContent from '@app/ingestV2/runDetails/RunDetailsContent';
import { formatDateTime } from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

import { useGetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

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

    return (
        <PageLayout title="Run Details" titlePill={titlePill} rightPanelContent={<AIChat />} topBreadcrumb={breadCrumb}>
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
