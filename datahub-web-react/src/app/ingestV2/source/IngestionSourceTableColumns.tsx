import { Avatar, Icon, Pill, Text, Tooltip, colors, typography } from '@components';
import { Button, Dropdown, Image, Typography } from 'antd';
import cronstrue from 'cronstrue';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';
import {
    RUNNING,
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingestV2/source/utils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { Owner } from '@types';

const PreviewImage = styled(Image)`
    max-height: 20px;
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

const TextContainer = styled(Typography.Text)`
    color: ${colors.gray[1700]};
`;

export const MenuItem = styled.div`
    display: flex;
    padding: 5px 50px 5px 5px;
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
`;

export const ActionIcons = styled.div`
    display: flex;
    justify-content: end;
    gap: 8px;

    div {
        border: 1px solid ${colors.gray[100]};
        border-radius: 200px;
        width: 24px;
        height: 24px;
        padding: 2px;
        color: ${colors.gray[1800]};
        :hover {
            cursor: pointer;
        }
    }
`;

const NameContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    width: 100%;
`;

const DisplayNameContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: calc(100% - 50px);
`;

const TruncatedText = styled(Text)`
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

interface TypeColumnProps {
    type: string;
    record: any;
}

export function NameColumn({ type, record }: TypeColumnProps) {
    const iconUrl = useGetSourceLogoUrl(type);
    const typeDisplayName = capitalizeFirstLetter(type);

    return (
        <NameContainer>
            {iconUrl && (
                <Tooltip overlay={typeDisplayName}>
                    <PreviewImage preview={false} src={iconUrl} alt={type || ''} />
                </Tooltip>
            )}
            <DisplayNameContainer>
                <TextContainer
                    ellipsis={{
                        tooltip: {
                            title: record.name,
                            color: 'white',
                            overlayInnerStyle: { color: colors.gray[1700] },
                            showArrow: false,
                        },
                    }}
                >
                    {record.name || ''}
                </TextContainer>
                {!iconUrl && typeDisplayName && <TruncatedText color="gray">{typeDisplayName}</TruncatedText>}
            </DisplayNameContainer>
            {record.cliIngestion && (
                <Tooltip title="This source is ingested from the command-line interface (CLI)">
                    <div>
                        <Pill label="CLI" color="blue" size="xs" />
                    </div>
                </Tooltip>
            )}
        </NameContainer>
    );
}

export function ScheduleColumn({ schedule, timezone }: { schedule: string; timezone?: string }) {
    let scheduleText: string;
    try {
        scheduleText = schedule && `Runs ${cronstrue.toString(schedule).toLowerCase()} (${timezone})`;
    } catch (e) {
        scheduleText = 'Invalid cron schedule';
        console.debug('Error parsing cron schedule', e);
    }
    return (
        <TextContainer
            ellipsis={{
                tooltip: {
                    title: scheduleText,
                    color: 'white',
                    overlayInnerStyle: { color: colors.gray[1700] },
                    showArrow: false,
                },
            }}
        >
            {scheduleText || 'Not scheduled'}
        </TextContainer>
    );
}

export function LastExecutionColumn({ time }: { time: number }) {
    const executionDate = new Date(time);
    const timeString = executionDate?.toLocaleTimeString();
    const [mainTime, timePeriod] = (timeString ?? '').split(' ');

    return (
        <>
            {time ? (
                <>
                    <Text>{`${executionDate.toLocaleDateString()}@ ${mainTime}`}</Text>
                    <Text>{timePeriod}</Text>
                </>
            ) : (
                'Never run'
            )}
        </>
    );
}

interface StatusProps {
    status: any;
    record: any;
    setFocusExecutionUrn: (urn: string) => void;
}

export function StatusColumn({ status, record, setFocusExecutionUrn }: StatusProps) {
    const icon = getExecutionRequestStatusIcon(status);
    const text = getExecutionRequestStatusDisplayText(status) || 'Pending...';
    const color = getExecutionRequestStatusDisplayColor(status);
    const { lastExecUrn } = record;
    return (
        <AllStatusWrapper>
            <StatusContainer>
                <StatusButton
                    data-testid="ingestion-source-table-status"
                    type="link"
                    onClick={() => setFocusExecutionUrn(lastExecUrn)}
                >
                    <Pill
                        customIconRenderer={() => <Icon icon={icon} source="phosphor" size="md" />}
                        label={text}
                        color={color}
                        size="md"
                    />
                </StatusButton>
            </StatusContainer>
        </AllStatusWrapper>
    );
}

export function OwnerColumn({ owners, entityRegistry }: { owners: Owner[]; entityRegistry: EntityRegistry }) {
    const ownerAvatars = owners.map((owner) => {
        return {
            name: entityRegistry.getDisplayName(owner.owner.type, owner.owner),
            imageUrl: owner.owner.editableProperties?.pictureLink,
        };
    });
    const singleOwner = owners.length === 1 ? owners[0].owner : undefined;
    return (
        <>
            {singleOwner && (
                <HoverEntityTooltip entity={singleOwner} showArrow={false}>
                    <Link to={`${entityRegistry.getEntityUrl(singleOwner.type, singleOwner.urn)}`}>
                        <Avatar
                            name={entityRegistry.getDisplayName(singleOwner.type, singleOwner)}
                            imageUrl={singleOwner.editableProperties?.pictureLink}
                            showInPill
                        />
                    </Link>
                </HoverEntityTooltip>
            )}
            {owners.length > 1 && <AvatarStack avatars={ownerAvatars} showRemainingNumber />}
        </>
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

type MenuOption = {
    key: string;
    label: React.ReactNode;
};

export function ActionsColumn({
    record,
    onEdit,
    setFocusExecutionUrn,
    onView,
    onExecute,
    onDelete,
}: ActionsColumnProps) {
    const items: MenuOption[] = [];

    if (!record.cliIngestion)
        items.push({
            key: '0',
            label: (
                <MenuItem
                    onClick={() => {
                        onEdit(record.urn);
                    }}
                >
                    Edit
                </MenuItem>
            ),
        });
    else
        items.push({
            key: '1',
            label: (
                <MenuItem
                    onClick={() => {
                        onView(record.urn);
                    }}
                >
                    View
                </MenuItem>
            ),
        });
    if (navigator.clipboard)
        items.push({
            key: '2',
            label: (
                <MenuItem
                    onClick={() => {
                        navigator.clipboard.writeText(record.urn);
                    }}
                >
                    Copy Urn
                </MenuItem>
            ),
        });
    if (record.lastExecStatus === RUNNING)
        items.push({
            key: '3',
            label: (
                <MenuItem
                    onClick={() => {
                        setFocusExecutionUrn(record.lastExecUrn);
                    }}
                >
                    Details
                </MenuItem>
            ),
        });
    items.push({
        key: '4',
        label: (
            <MenuItem
                onClick={() => {
                    onDelete(record.urn);
                }}
            >
                <Text color="red">Delete </Text>
            </MenuItem>
        ),
    });

    return (
        <>
            <ActionIcons>
                {!record.cliIngestion && record.lastExecStatus !== RUNNING && (
                    <Icon icon="Play" source="phosphor" onClick={() => onExecute(record.urn)} />
                )}
                <Dropdown menu={{ items }} trigger={['click']}>
                    <Icon icon="DotsThreeVertical" source="phosphor" />
                </Dropdown>
            </ActionIcons>
        </>
    );
}
