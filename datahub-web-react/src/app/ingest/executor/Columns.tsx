import React from 'react';
import { Popover, Typography } from 'antd';
import { RemoteExecutor } from '@src/types.generated';
import { formatDuration } from '@src/app/shared/formatDuration';
import { CheckCircle, Stop, WarningCircle } from 'phosphor-react';

export function TimeColumn(time: string) {
    const date = time && new Date(time);
    const localTime = date && `${date.toLocaleDateString()} at ${date.toLocaleTimeString()}`;
    return <Typography.Text>{localTime || 'None'}</Typography.Text>;
}

const getActiveLabel = (numActive?: number) => (
    <Popover
        content={
            <Typography.Text>
                {numActive === undefined ? 'This executor is' : 'Executors that are'} currently active.
            </Typography.Text>
        }
    >
        <Typography.Text type="success" style={{ fontWeight: 700, marginRight: 8 }}>
            <CheckCircle size={14} style={{ marginBottom: -2 }} />
            {numActive !== undefined ? ` ${numActive}` : ''} Active
        </Typography.Text>
    </Popover>
);
const getStaleLabel = (numStale?: number) => (
    <Popover
        content={
            <Typography.Text>
                <strong>
                    {numStale === undefined ? 'This executor has' : 'Executors that have'} not reported a heartbeat in
                    over an hour.
                </strong>
                <br />
                You may want to reach out to your customer success representative for assistance.
            </Typography.Text>
        }
    >
        <Typography.Text type="warning" style={{ fontWeight: 700, marginRight: 8 }}>
            <WarningCircle size={14} style={{ marginBottom: -2 }} />
            {numStale !== undefined ? ` ${numStale}` : ''} Stale
        </Typography.Text>
    </Popover>
);
const getStoppedLabel = (numStopped?: number) => (
    <Popover
        content={
            <Typography.Text>
                {numStopped === undefined ? 'This executor has' : 'Executors that have'} been safely stopped and
                succesfully reported termination.
            </Typography.Text>
        }
    >
        <Typography.Text type="secondary" style={{ fontWeight: 700, marginRight: 8 }}>
            <Stop size={14} style={{ marginBottom: -2 }} />
            {numStopped !== undefined ? ` ${numStopped}` : ''} Stopped
        </Typography.Text>
    </Popover>
);

export function StatusColumn({
    executorExpired,
    executorStopped,
    executorUptime,
}: {
    executorExpired: number;
    executorStopped: number;
    executorUptime: number;
}) {
    let status: any;
    if (executorExpired) {
        status = getStaleLabel();
    } else if (executorStopped) {
        status = getStoppedLabel();
    } else if (executorUptime) {
        status = (
            <>
                {getActiveLabel()} <span style={{ opacity: 0.5 }}>({formatDuration(executorUptime)})</span>
            </>
        );
    }
    return <div>{status || <Typography.Text>None</Typography.Text>}</div>;
}

export function PoolStatusColumn({ executors }: { executors: RemoteExecutor[] }) {
    if (!executors.length) {
        return (
            <Popover
                content={
                    <Typography.Text type="secondary">
                        Reference the{' '}
                        <Typography.Link href="https://datahubproject.io/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor/">
                            remote ingestion setup documentation
                        </Typography.Link>{' '}
                        to deploy executors for this pool.
                    </Typography.Text>
                }
            >
                <Typography.Text type="secondary">No executors deployed yet.</Typography.Text>
            </Popover>
        );
    }

    const numActive = executors.filter(
        (executor) => !executor.executorExpired && !executor.executorStopped && executor.executorUptime,
    ).length;
    const numStale = executors.filter((executor) => executor.executorExpired).length;
    const numStopped = executors.filter((executor) => executor.executorStopped).length;
    const status = [
        numActive && getActiveLabel(numActive),
        numStale && getStaleLabel(numStale),
        numStopped && getStoppedLabel(numStopped),
    ].filter(Boolean);
    return <div>{status || <Typography.Text type="secondary">None</Typography.Text>}</div>;
}

export function DetailsColumn({ executor }: { executor: RemoteExecutor }) {
    return (
        <Popover
            content={
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                    <Typography.Text>
                        <b>Location:</b> {executor.executorInternal ? 'Hosted on DataHub Cloud' : 'Hosted by customer'}
                    </Typography.Text>
                    <Typography.Text>
                        <b>Host:</b> {executor.executorHostname}
                    </Typography.Text>
                    <Typography.Text>
                        <b>Address:</b> {executor.executorAddress}
                    </Typography.Text>
                    <Typography.Text>
                        <b>Logs enabled:</b> {executor.logDeliveryEnabled ? 'Yes' : 'No'}
                    </Typography.Text>
                    <Typography.Text>
                        <b>Embedded mode:</b> {executor.executorEmbedded ? 'Yes' : 'No'}
                    </Typography.Text>
                </div>
            }
        >
            <Typography.Link>See more</Typography.Link>
        </Popover>
    );
}
