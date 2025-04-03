import React, { useEffect, useState } from 'react';
import { Button, Dropdown, Input, message, Popover, Typography } from 'antd';
import { ExecutionRequest, RemoteExecutor } from '@src/types.generated';
import { formatDuration } from '@src/app/shared/formatDuration';
import { Check, CheckCircle, Pencil, Plus, StopCircle, WarningCircle } from 'phosphor-react';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import Loading from '@src/app/shared/Loading';
import { useGetIngestionSourceNameLazyQuery } from '@src/graphql/ingestion.generated';
import { useUpdateRemoteExecutorPoolMutation } from '@src/graphql/remote_executor.saas.generated';
import { checkIsExecutionRequestRunning, getExecutionRequestStatusDisplayText } from '../source/utils';
import { ExecutionDetailsModal } from '../source/executions/ExecutionRequestDetailsModal';
import { checkIsExecutorStale, EXECUTOR_STALE_THRESHOLD_MS } from './utils';

export function TimeColumn(reportedAt: number) {
    const date = reportedAt && new Date(reportedAt);
    const localTime = date && `${date.toLocaleDateString()} at ${date.toLocaleTimeString()}`;
    return <Typography.Text>{localTime || 'None'}</Typography.Text>;
}

const LinkButton = styled(Button)`
    padding: 0;
    margin: 0;
    border: none;
    background: none;
    box-shadow: none;
    flex-direction: row;
    display: flex;
    align-items: center;
    color: ${colors.blue[500]};
`;

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
                    over {EXECUTOR_STALE_THRESHOLD_MS / 60000} minutes.
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
            <StopCircle size={14} style={{ marginBottom: -2 }} />
            {numStopped !== undefined ? ` ${numStopped}` : ''} Stopped
        </Typography.Text>
    </Popover>
);

export function StatusColumn({
    reportedAt,
    executorStopped,
    executorExpired,
    executorUptime,
}: {
    reportedAt: number;
    executorStopped: boolean;
    executorExpired: boolean;
    executorUptime: number;
}) {
    let status: any;
    if (checkIsExecutorStale({ reportedAt, executorStopped, executorExpired })) {
        status = getStaleLabel();
    } else if (executorStopped) {
        status = getStoppedLabel();
    } else if (executorUptime) {
        status = (
            <>
                {getActiveLabel()} <span style={{ opacity: 0.5 }}>({formatDuration(executorUptime * 1000)})</span>
            </>
        );
    }
    return <div>{status || <Typography.Text>None</Typography.Text>}</div>;
}

const PoolDescriptionWrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
`;

const PoolDescriptionText = styled(Typography.Text)`
    max-width: 140px;
    display: inline-block;
`;

const PoolDescriptionActionButton = styled(Button)`
    padding: 0;
    height: auto;
    margin-left: 4px;
    vertical-align: middle;
`;

export function PoolDescriptionColumn({
    description,
    urn,
    onUpdate,
}: {
    description: string;
    urn: string;
    onUpdate: () => void;
}) {
    const [isEditing, setIsEditing] = useState(false);
    const [inputValue, setInputValue] = useState(description);
    const [updateRemoteExecutorPool] = useUpdateRemoteExecutorPoolMutation();

    const onUpdatePool = async () => {
        try {
            await updateRemoteExecutorPool({
                variables: {
                    input: {
                        urn,
                        description: inputValue,
                    },
                },
            });
            setIsEditing(false);
            message.success('Pool description updated');
            setTimeout(onUpdate, 500);
        } catch (e) {
            console.error('Failed to update pool description:', e);
            message.error('Failed to update pool description');
        }
    };

    return isEditing ? (
        // ---------------------- editing state ---------------------- //
        <PoolDescriptionWrapper>
            <Input.TextArea
                rows={2}
                style={{ width: 140 }}
                autoFocus
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
            />
            <PoolDescriptionActionButton type="text" onClick={onUpdatePool}>
                <Check size={16} />
            </PoolDescriptionActionButton>
        </PoolDescriptionWrapper>
    ) : (
        // ---------------------- viewing state ---------------------- //
        <PoolDescriptionWrapper>
            <PoolDescriptionText style={{ opacity: description ? 1 : 0.5 }}>
                {description || 'No description'}
            </PoolDescriptionText>
            <PoolDescriptionActionButton type="text" onClick={() => setIsEditing(true)}>
                {description ? <Pencil size={12} /> : <Plus size={12} />}
            </PoolDescriptionActionButton>
        </PoolDescriptionWrapper>
    );
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
        (executor) =>
            !executor.executorExpired &&
            !executor.executorStopped &&
            executor.executorUptime &&
            !checkIsExecutorStale(executor),
    ).length;
    const numStale = executors.filter(checkIsExecutorStale).length;
    const numStopped = executors.filter((executor) => executor.executorStopped && !executor.executorExpired).length;
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
                    {/* Hiding IP address for now as it's not useful information and unecessary to expose */}
                    {/* <Typography.Text>
                        <b>Address:</b> {executor.executorAddress}
                    </Typography.Text> */}
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

const ExecutionRequestPreview = ({ task, isVisible }: { task: ExecutionRequest; isVisible: boolean }) => {
    const [getName, { data }] = useGetIngestionSourceNameLazyQuery({
        variables: { urn: task.input.source.ingestionSource || '' },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (!isVisible || data?.ingestionSource?.name) {
            return;
        }
        getName();
    }, [isVisible, data?.ingestionSource?.name, getName]);

    const sourceName =
        data?.ingestionSource?.name || task.input.source.ingestionSource || task.input.source.type || 'Unknown source';

    return (
        <div>
            Requested at {new Date(task.input.requestedAt).toLocaleTimeString()} for <strong>{sourceName}</strong>&nbsp;
            <span style={{ opacity: 0.5 }}>
                ({task.result?.status ? getExecutionRequestStatusDisplayText(task.result.status) : 'Pending...'})
            </span>
        </div>
    );
};

export function ActiveTasksColumn({ executor }: { executor: RemoteExecutor }) {
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<string | undefined>(undefined);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);

    const recentTasks = executor.recentExecutions?.executionRequests;

    const activeTasks = recentTasks?.filter((task) => checkIsExecutionRequestRunning(task.result));
    if (!activeTasks?.length) {
        return <Typography.Text>None</Typography.Text>;
    }

    const taskOptions = activeTasks.map((task) => ({
        key: task.urn,
        label: <ExecutionRequestPreview task={task} isVisible={isDropdownOpen} />,
        onClick: () => {
            setFocusExecutionUrn(task.urn);
        },
    }));

    return (
        <>
            <Dropdown
                placement="bottom"
                menu={{ items: taskOptions }}
                onOpenChange={(isOpen) => setIsDropdownOpen(isOpen)}
            >
                <LinkButton>
                    <Loading marginTop={0} height={12} />
                    <div style={{ marginLeft: 4 }}>
                        {activeTasks.length} running job{activeTasks.length > 1 ? 's' : ''}
                    </div>
                </LinkButton>
            </Dropdown>
            {focusExecutionUrn && (
                <ExecutionDetailsModal
                    urn={focusExecutionUrn}
                    open={focusExecutionUrn !== undefined}
                    onClose={() => setFocusExecutionUrn(undefined)}
                />
            )}
        </>
    );
}
