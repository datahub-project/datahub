import React from 'react';
import { CopyOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import { Text, Tooltip } from '@components';
import styled from 'styled-components';
import CustomAvatar from '@src/app/shared/avatar/CustomAvatar';
import { Link } from 'react-router-dom';
import { CreatedByContainer } from '@src/app/govern/structuredProperties/styledComponents';
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

type Actor = {
    actorUrn: string;
    displayName: string;
    displayUrl: string;
};

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

// TODO: This will be soon exported into a component
const UserPill = ({ actor }: { actor: Actor }) => (
    <Link to={actor?.displayUrl}>
        <CreatedByContainer>
            <CustomAvatar size={16} name={actor?.displayName} hideTooltip />
            <Text type="div" color="gray" size="sm" weight="semiBold">
                {actor?.displayName}
            </Text>
        </CreatedByContainer>
    </Link>
);

interface SourceColumnProps {
    source: string;
    actor?: Actor;
}

export function SourceColumn({ source, actor }: SourceColumnProps) {
    switch (source) {
        case MANUAL_INGESTION_SOURCE:
            if (!actor) return <>Manual Execution</>;
            return (
                <UserContainer>
                    Manual Execution by <UserPill actor={actor} />
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
