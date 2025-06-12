import { Avatar, Icon, Pill, Text, Tooltip, colors } from '@components';
import { Image, Typography } from 'antd';
import cronstrue from 'cronstrue';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { EXECUTION_REQUEST_STATUS_RUNNING } from '@app/ingestV2/executions/constants';
import BaseActionsColumn, { MenuItem } from '@app/ingestV2/shared/components/columns/BaseActionsColumn';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';
import { capitalizeMonthsAndDays, formatTimezone } from '@app/ingestV2/source/utils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { EntityType, Owner } from '@types';

const PreviewImage = styled(Image)`
    max-height: 20px;
    width: auto;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

const TextContainer = styled(Typography.Text)`
    color: ${colors.gray[1700]};
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
        const text = schedule && `${cronstrue.toString(schedule).toLowerCase()} (${formatTimezone(timezone)})`;
        const cleanedText = text.replace(/^at /, '');
        const finalText = capitalizeFirstLetter(capitalizeMonthsAndDays(cleanedText));
        scheduleText = finalText ?? '-';
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
            {scheduleText || '-'}
        </TextContainer>
    );
}

export function OwnerColumn({ owners, entityRegistry }: { owners: Owner[]; entityRegistry: EntityRegistry }) {
    const ownerAvatars = owners.map((owner) => {
        return {
            name: entityRegistry.getDisplayName(owner.owner.type, owner.owner),
            imageUrl: owner.owner.editableProperties?.pictureLink,
            type: owner.owner.type,
            urn: owner.owner.urn,
        };
    });
    const singleOwner = owners.length === 1 ? owners[0].owner : undefined;

    if (owners.length === 0) return <>-</>;

    return (
        <>
            {singleOwner && (
                <HoverEntityTooltip entity={singleOwner} showArrow={false}>
                    <Link
                        to={`${entityRegistry.getEntityUrl(singleOwner.type, singleOwner.urn)}`}
                        onClick={(e) => {
                            e.stopPropagation();
                        }}
                    >
                        <Avatar
                            name={entityRegistry.getDisplayName(singleOwner.type, singleOwner)}
                            imageUrl={singleOwner.editableProperties?.pictureLink}
                            showInPill
                            isGroup={singleOwner.type === EntityType.CorpGroup}
                        />
                    </Link>
                </HoverEntityTooltip>
            )}
            {owners.length > 1 && (
                <AvatarStackWithHover avatars={ownerAvatars} showRemainingNumber entityRegistry={entityRegistry} />
            )}
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
    if (record.lastExecStatus === EXECUTION_REQUEST_STATUS_RUNNING)
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
        <BaseActionsColumn
            dropdownItems={items}
            extraActions={
                !record.cliIngestion && record.lastExecStatus !== EXECUTION_REQUEST_STATUS_RUNNING ? (
                    <Icon
                        icon="Play"
                        source="phosphor"
                        onClick={(e) => {
                            e.stopPropagation();
                            onExecute(record.urn);
                        }}
                    />
                ) : null
            }
        />
    );
}
