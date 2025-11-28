import { Breadcrumb, colors } from '@components';
import { Divider } from 'antd';
import React, { useEffect } from 'react';
import { useLocation, useParams } from 'react-router';
import styled from 'styled-components';

import RunDetailsContent from '@app/ingestV2/runDetails/RunDetailsContent';
import { formatDateTime } from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

const VerticalDivider = styled(Divider)`
    color: ${colors.gray[100]};
    height: 16px;
    width: 2px;
    margin: 0 4px;
`;

export default function IngestionRunDetailsPage() {
    const { urn } = useParams<{ urn: string }>();

    const { state } = useLocation();
    const [fromUrl, setFromUrl] = React.useState<string>();
    const [name, setName] = React.useState<string>();
    const [runTime, setRunTime] = React.useState<number>();

    const [titlePill, setTitlePill] = React.useState<React.ReactNode>(null);

    useEffect(() => {
        if (state?.fromUrl) {
            setFromUrl(state.fromUrl);
        }
        if (state?.name) {
            setName(state.name);
        }
        if (state?.runTime) {
            setRunTime(state.runTime);
        }
    }, [state]);

    const breadCrumb = (
        <Breadcrumb
            items={[
                {
                    label: fromUrl === tabUrlMap[TabType.RunHistory] ? 'Run history' : 'Manage Data Sources',
                    href: fromUrl,
                    separator: <VerticalDivider type="vertical" />,
                },
                {
                    label: name,
                },
                {
                    label: formatDateTime(runTime),
                    isCurrent: true,
                },
            ]}
        />
    );

    return (
        <PageLayout title="Run Details" titlePill={titlePill} rightPanelContent={<AIChat />} topBreadcrumb={breadCrumb}>
            <RunDetailsContent urn={urn} setTitlePill={setTitlePill} />
        </PageLayout>
    );
}
