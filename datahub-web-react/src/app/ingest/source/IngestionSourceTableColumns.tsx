import { blue } from '@ant-design/colors';
import { CodeOutlined, CopyOutlined, DeleteOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Image, Typography } from 'antd';
import cronstrue from 'cronstrue';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import useGetSourceLogoUrl from '@app/ingest/source/builder/useGetSourceLogoUrl';
import {
    RUNNING,
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingest/source/utils';
import { capitalizeFirstLetter } from '@app/shared/textUtil';

const PreviewImage = styled(Image)`
    max-height: 28px;
    width: auto;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const AllStatusWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const StatusButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const TypeWrapper = styled.div`
    align-items: center;
    display: flex;
`;

const CliBadge = styled.span`
    margin-left: 20px;
    border-radius: 15px;
    border: 1px solid ${ANTD_GRAY[8]};
    padding: 1px 4px;
    font-size: 10px;

    font-size: 8px;
    font-weight: bold;
    letter-spacing: 0.5px;
    border: 1px solid ${blue[6]};
    color: ${blue[6]};

    svg {
        display: none;
        margin-right: 5px;
    }
`;
const StatusText = styled(Typography.Text)`
    font-weight: bold;
    margin-left: 8px;
    color: ${(props) => props.color};
    &:hover {
        text-decoration: underline;
      },
`;
interface TypeColumnProps {
    type: string;
    record: any;
}

export function TypeColumn({ type, record }: TypeColumnProps) {
    const iconUrl = useGetSourceLogoUrl(type);
    const typeDisplayName = capitalizeFirstLetter(type);

    return (
        <TypeWrapper>
            {iconUrl ? (
                <Tooltip overlay={typeDisplayName}>
                    <PreviewImage preview={false} src={iconUrl} alt={type || ''} />
                </Tooltip>
            ) : (
                <Typography.Text strong>{typeDisplayName}</Typography.Text>
            )}
            {record.cliIngestion && (
                <Tooltip title="This source is ingested from the command-line interface (CLI)">
                    <CliBadge>
                        <CodeOutlined />
                        CLI
                    </CliBadge>
                </Tooltip>
            )}
        </TypeWrapper>
    );
}

export function LastExecutionColumn({ time }: { time: number }) {
    const executionDate = time && new Date(time);
    const localTime = executionDate && `${executionDate.toLocaleDateString()} at ${executionDate.toLocaleTimeString()}`;
    return <Typography.Text type="secondary">{localTime ? `Last run ${localTime}` : 'Never run'}</Typography.Text>;
}

export function ScheduleColumn(schedule: any, record: any) {
    let tooltip: string;
    try {
        tooltip = schedule && `Runs ${cronstrue.toString(schedule).toLowerCase()} (${record.timezone})`;
    } catch (e) {
        tooltip = 'Invalid cron schedule';
        console.debug('Error parsing cron schedule', e);
    }
    return (
        <Tooltip title={tooltip || 'Not scheduled'}>
            <Typography.Text code>{schedule || 'None'}</Typography.Text>
        </Tooltip>
    );
}

interface LastStatusProps {
    status: any;
    record: any;
    setFocusExecutionUrn: (urn: string) => void;
}

export function LastStatusColumn({ status, record, setFocusExecutionUrn }: LastStatusProps) {
    const Icon = getExecutionRequestStatusIcon(status);
    const text = getExecutionRequestStatusDisplayText(status);
    const color = getExecutionRequestStatusDisplayColor(status);
    const { lastExecTime, lastExecUrn } = record;
    return (
        <AllStatusWrapper>
            <StatusContainer>
                {Icon && <Icon style={{ color, fontSize: 14 }} />}
                <StatusButton
                    data-testid="ingestion-source-table-status"
                    type="link"
                    onClick={() => setFocusExecutionUrn(lastExecUrn)}
                >
                    <StatusText color={color}>{text || 'Pending...'}</StatusText>
                </StatusButton>
            </StatusContainer>
            <LastExecutionColumn time={lastExecTime} />
        </AllStatusWrapper>
    );
}

interface ActionsColumnProps {
    record: any;
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
}

export function ActionsColumn({
    record,
    onEdit,
    setFocusExecutionUrn,
    onView,
    onExecute,
    onDelete,
}: ActionsColumnProps) {
    return (
        <ActionButtonContainer>
            {navigator.clipboard && (
                <Tooltip title="Copy Ingestion Source URN">
                    <Button
                        style={{ marginRight: 16 }}
                        icon={<CopyOutlined />}
                        onClick={() => {
                            navigator.clipboard.writeText(record.urn);
                        }}
                    />
                </Tooltip>
            )}
            {!record.cliIngestion && (
                <Button
                    data-testid="ingestion-source-table-edit-button"
                    style={{ marginRight: 16 }}
                    onClick={() => onEdit(record.urn)}
                >
                    EDIT
                </Button>
            )}
            {record.cliIngestion && (
                <Button style={{ marginRight: 16 }} onClick={() => onView(record.urn)}>
                    VIEW
                </Button>
            )}
            {record.lastExecStatus !== RUNNING && (
                <Button
                    disabled={record.cliIngestion}
                    style={{ marginRight: 16 }}
                    onClick={() => onExecute(record.urn)}
                >
                    RUN
                </Button>
            )}
            {record.lastExecStatus === RUNNING && (
                <Button style={{ marginRight: 16 }} onClick={() => setFocusExecutionUrn(record.lastExecUrn)}>
                    DETAILS
                </Button>
            )}
            <Button
                data-testid={`delete-ingestion-source-${record.name}`}
                onClick={() => onDelete(record.urn)}
                type="text"
                shape="circle"
                danger
            >
                <DeleteOutlined />
            </Button>
        </ActionButtonContainer>
    );
}
