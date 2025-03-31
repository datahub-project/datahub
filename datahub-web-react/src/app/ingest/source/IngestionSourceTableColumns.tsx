import { blue } from '@ant-design/colors';
import { Dropdown, Image, Typography } from 'antd';
import { Tooltip, Button } from '@components';
import { colors, typography } from '@src/alchemy-components';
import { Copy, PencilSimple, Trash, DotsThreeVertical, Code } from 'phosphor-react';
import cronstrue from 'cronstrue';
import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import useGetSourceLogoUrl from './builder/useGetSourceLogoUrl';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
    RUNNING,
} from './utils';

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

const StatusButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    gap: 16px;
    align-items: center;
`;

const StyledDotsThreeVertical = styled(DotsThreeVertical)`
    color: ${colors.gray[1800]};
    cursor: pointer;
    transition: color 0.2s ease-in-out;

    &:hover {
        color: ${colors.violet[500]};
    }
`;

const MenuItem = styled.div`
    display: flex;
    padding: 5px 12px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
    align-items: center;
    gap: 8px;
    white-space: nowrap;

    &.danger {
        color: ${colors.red[1200]};
    }

    &:hover {
        cursor: pointer;
    }

    svg {
        color: ${colors.gray[1800]};
        font-size: 16px;
    }

    &.danger svg {
        color: ${colors.red[1200]};
    }
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
                        <Code size={16} />
                        CLI
                    </CliBadge>
                </Tooltip>
            )}
        </TypeWrapper>
    );
}

export function LastExecutionColumn(time: any) {
    const executionDate = time && new Date(time);
    const localTime = executionDate && `${executionDate.toLocaleDateString()} at ${executionDate.toLocaleTimeString()}`;
    return <Typography.Text>{localTime || 'None'}</Typography.Text>;
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
    const StatusIcon = getExecutionRequestStatusIcon(status);
    const text = getExecutionRequestStatusDisplayText(status);
    const color = getExecutionRequestStatusDisplayColor(status);
    return (
        <StatusContainer>
            {StatusIcon && <StatusIcon style={{ color, fontSize: 14 }} />}
            <StatusButton
                data-testid="ingestion-source-table-status"
                variant="text"
                size="sm"
                onClick={() => setFocusExecutionUrn(record.lastExecUrn)}
            >
                <StatusText color={color}>{text || 'Pending...'}</StatusText>
            </StatusButton>
        </StatusContainer>
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
    const items = [
        {
            key: 'copy',
            label: (
                <MenuItem onClick={() => navigator.clipboard?.writeText(record.urn)} data-test-id="copy-urn-action">
                    <Copy weight="regular" size={16} />
                    Copy URN
                </MenuItem>
            ),
        },
    ];

    if (!record.cliIngestion) {
        items.push({
            key: 'edit',
            label: (
                <MenuItem onClick={() => onEdit(record.urn)} data-test-id="edit-action">
                    <PencilSimple weight="regular" size={16} />
                    Edit
                </MenuItem>
            ),
        });
    }

    items.push({
        key: 'delete',
        label: (
            <MenuItem className="danger" onClick={() => onDelete(record.urn)} data-test-id="delete-action">
                <Trash weight="regular" size={16} />
                Delete
            </MenuItem>
        ),
    });

    return (
        <ActionButtonContainer>
            {record.cliIngestion && (
                <Button variant="text" onClick={() => onView(record.urn)} data-test-id="view-action">
                    VIEW
                </Button>
            )}
            {record.lastExecStatus !== RUNNING && (
                <Button
                    variant="text"
                    size="sm"
                    disabled={record.cliIngestion}
                    onClick={() => onExecute(record.urn)}
                    data-test-id="run-action"
                >
                    RUN
                </Button>
            )}
            {record.lastExecStatus === RUNNING && (
                <Button
                    variant="text"
                    onClick={() => setFocusExecutionUrn(record.lastExecUrn)}
                    data-test-id="details-action"
                >
                    DETAILS
                </Button>
            )}
            <Dropdown menu={{ items }} trigger={['click']} placement="bottomRight">
                <StyledDotsThreeVertical weight="bold" size={20} data-test-id="more-actions" />
            </Dropdown>
        </ActionButtonContainer>
    );
}
