import { CopyOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import UserWithAvatar from '@app/entity/shared/components/styled/UserWithAvatar';
import {
    CLI_INGESTION_SOURCE,
    MANUAL_INGESTION_SOURCE,
    RUNNING,
    SCHEDULED_INGESTION_SOURCE,
    SUCCESS,
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingest/source/utils';

import { CorpUser } from '@types';

const UserContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    wrap: auto;
`;

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const StatusButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

export function TimeColumn(time: string) {
    const date = time && new Date(time);
    const localTime = date && `${date.toLocaleDateString()} at ${date.toLocaleTimeString()}`;
    return <Typography.Text>{localTime || 'None'}</Typography.Text>;
}

interface StatusColumnProps {
    status: string;
    record: any;
    setFocusExecutionUrn: (urn: string) => void;
}

export function StatusColumn({ status, record, setFocusExecutionUrn }: StatusColumnProps) {
    const Icon = getExecutionRequestStatusIcon(status);
    const text = getExecutionRequestStatusDisplayText(status);
    const color = getExecutionRequestStatusDisplayColor(status);
    return (
        <StatusContainer>
            {Icon && <Icon style={{ color, fontSize: 14 }} />}
            <StatusButton type="link" onClick={() => setFocusExecutionUrn(record.urn)}>
                <Typography.Text strong style={{ color, marginLeft: 8 }}>
                    {text || 'Pending...'}
                </Typography.Text>
            </StatusButton>
        </StatusContainer>
    );
}

interface SourceColumnProps {
    source: string;
    actor?: CorpUser;
}

export function SourceColumn({ source, actor }: SourceColumnProps) {
    switch (source) {
        case MANUAL_INGESTION_SOURCE:
            if (!actor) return <>Manual Execution</>;

            return (
                <UserContainer>
                    Manual Execution by <UserWithAvatar user={actor} />
                </UserContainer>
            );

        case SCHEDULED_INGESTION_SOURCE:
            return <span>Scheduled Execution</span>;

        case CLI_INGESTION_SOURCE:
            return <span>CLI Execution</span>;

        default:
            return <span>N/A</span>;
    }
}

interface ButtonsColumnProps {
    record: any;
    handleViewDetails: (urn: string) => void;
    handleCancelExecution: (urn: string) => void;
    handleRollbackExecution: (runId: string) => void;
}

export function ButtonsColumn({
    record,
    handleViewDetails,
    handleCancelExecution,
    handleRollbackExecution,
}: ButtonsColumnProps) {
    return (
        <div style={{ display: 'flex', justifyContent: 'right' }}>
            {record.urn && navigator.clipboard && (
                <Tooltip title="Copy Execution Request URN">
                    <Button
                        style={{ marginRight: 16 }}
                        icon={<CopyOutlined />}
                        onClick={() => {
                            navigator.clipboard.writeText(record.urn);
                        }}
                    />
                </Tooltip>
            )}
            {record.duration && (
                <Button style={{ marginRight: 16 }} onClick={() => handleViewDetails(record.urn)}>
                    DETAILS
                </Button>
            )}
            {record.status === RUNNING && (
                <Button style={{ marginRight: 16 }} onClick={() => handleCancelExecution(record.urn)}>
                    CANCEL
                </Button>
            )}
            {record.status === SUCCESS && record.showRollback && (
                <Button style={{ marginRight: 16 }} onClick={() => handleRollbackExecution(record.id)}>
                    ROLLBACK
                </Button>
            )}
        </div>
    );
}
