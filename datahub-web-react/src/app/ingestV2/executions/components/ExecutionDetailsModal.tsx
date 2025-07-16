import { Modal, message } from 'antd';
import React, { useEffect, useState } from 'react';

import { Tab, Tabs } from '@components/components/Tabs/Tabs';

import { LogsTab } from '@app/ingestV2/executions/components/LogsTab';
import { RecipeTab } from '@app/ingestV2/executions/components/RecipeTab';
import { SummaryTab } from '@app/ingestV2/executions/components/SummaryTab';
import { EXECUTION_REQUEST_STATUS_RUNNING } from '@app/ingestV2/executions/constants';
import { getIngestionSourceStatus } from '@app/ingestV2/source/utils';
import { Message } from '@app/shared/Message';

import { useGetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';
import { ExecutionRequestResult } from '@types';

const modalBodyStyle = {
    padding: 0,
};

type Props = {
    urn: string;
    open: boolean;
    onClose: () => void;
};

const enum TabType {
    Summary = 'Summary',
    Logs = 'Logs',
    Recipe = 'Recipe',
}

export const ExecutionDetailsModal = ({ urn, open, onClose }: Props) => {
    const { data, loading, error, refetch } = useGetIngestionExecutionRequestQuery({ variables: { urn } });
    const result = data?.executionRequest?.result as Partial<ExecutionRequestResult>;
    const status = getIngestionSourceStatus(result);
    const [selectedTab, setSelectedTab] = useState<TabType>(TabType.Summary);

    useEffect(() => {
        const interval = setInterval(() => {
            if (status === EXECUTION_REQUEST_STATUS_RUNNING) refetch();
        }, 2000);

        return () => clearInterval(interval);
    });

    const tabs: Tab[] = [
        {
            component: (
                <SummaryTab urn={urn} status={status} result={result} data={data} onTabChange={setSelectedTab} />
            ),
            key: TabType.Summary,
            name: TabType.Summary,
        },
        {
            component: <LogsTab urn={urn} data={data} />,
            key: TabType.Logs,
            name: TabType.Logs,
        },
        {
            component: <RecipeTab data={data} />,
            key: TabType.Recipe,
            name: TabType.Recipe,
        },
    ];

    return (
        <Modal
            width="1400px"
            bodyStyle={modalBodyStyle}
            title="Status Details"
            open={open}
            onCancel={onClose}
            buttons={[{ text: 'Close', variant: 'outline', onClick: onClose }]}
        >
            {!data && loading && <Message type="loading" content="Loading execution run details..." />}
            {error && message.error('Failed to load execution run details :(')}
            <Tabs
                tabs={tabs}
                selectedTab={selectedTab}
                onChange={(tab) => setSelectedTab(tab as TabType)}
                getCurrentUrl={() => window.location.pathname}
                scrollToTopOnChange
                maxHeight="80vh"
                stickyHeader
                addPaddingLeft
            />
        </Modal>
    );
};
