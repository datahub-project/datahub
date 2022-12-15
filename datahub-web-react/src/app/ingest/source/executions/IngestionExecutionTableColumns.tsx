import React from 'react';
import { CopyOutlined } from '@ant-design/icons';
import { Button, Typography, Tooltip } from 'antd';
import styled from 'styled-components';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusIcon,
    getExecutionRequestStatusDisplayText,
    CLI_INGESTION_SOURCE,
    SCHEDULED_INGESTION_SOURCE,
    MANUAL_INGESTION_SOURCE,
    RUNNING,
    SUCCESS,
} from '../utils';

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
    return <Typography.Text>{localTime || 'N/A'}</Typography.Text>;
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
            {Icon && <Icon style={{ color }} />}
            <StatusButton type="link" onClick={() => setFocusExecutionUrn(record.urn)}>
                <Typography.Text strong style={{ color, marginLeft: 8 }}>
                    {text || 'N/A'}
                </Typography.Text>
            </StatusButton>
        </StatusContainer>
    );
}

export function SourceColumn(source: string) {
    return (
        (source === MANUAL_INGESTION_SOURCE && 'Manual Execution') ||
        (source === SCHEDULED_INGESTION_SOURCE && 'Scheduled Execution') ||
        (source === CLI_INGESTION_SOURCE && 'CLI Execution') ||
        'N/A'
    );
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
