import { Avatar, CellHoverWrapper, Icon, Pill, Text, Tooltip, colors } from '@components';
import { Image, Typography } from 'antd';
import cronstrue from 'cronstrue';
import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';
import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { EXECUTION_REQUEST_STATUS_LOADING, EXECUTION_REQUEST_STATUS_RUNNING } from '@app/ingestV2/executions/constants';
import BaseActionsColumn, { MenuItem } from '@app/ingestV2/shared/components/columns/BaseActionsColumn';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';
import { IngestionSourceTableData } from '@app/ingestV2/source/types';
import { capitalizeMonthsAndDays, formatTimezone } from '@app/ingestV2/source/utils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { capitalizeFirstLetter, capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { Owner } from '@types';

const PreviewImage = styled(Image)`
    max-height: 20px;
    width: auto;
    max-width: 28px;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

const TextContainer = styled(Typography.Text)<{ $shouldUnderline?: boolean }>`
    color: ${colors.gray[1700]};
    ${(props) =>
        props.$shouldUnderline &&
        `
            :hover {
                text-decoration: underline;
            }
        `}
`;

const SourceNameText = styled(Typography.Text)<{ $shouldUnderline?: boolean }>`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    line-height: 1.3;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    word-break: break-word;
    white-space: normal;
    text-overflow: unset;

    &.ant-typography {
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        overflow: hidden;
        white-space: normal;
    }

    ${(props) =>
        props.$shouldUnderline &&
        `
            :hover {
                text-decoration: underline;
            }
        `}
`;

const SourceTypeText = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1700]};
    line-height: normal;
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

interface NameColumnProps {
    type: string;
    record: any;
    onNameClick?: () => void;
}

export function NameColumn({ type, record, onNameClick }: NameColumnProps) {
    const iconUrl = useGetSourceLogoUrl(type);
    const typeDisplayName = capitalizeFirstLetter(type);
    const textRef = useRef<HTMLDivElement>(null);
    const [showTooltip, setShowTooltip] = useState(false);

    useEffect(() => {
        const element = textRef.current;
        if (element) {
            const isOverflowing = element.scrollHeight > element.clientHeight;
            setShowTooltip(isOverflowing);
        }
    }, [record.name]);

    const textElement = (
        <SourceNameText
            ref={textRef}
            onClick={(e) => {
                if (onNameClick) {
                    e.stopPropagation();
                    onNameClick();
                }
            }}
            $shouldUnderline={!!onNameClick}
            data-testid="ingestion-source-name"
        >
            {record.name || ''}
        </SourceNameText>
    );

    return (
        <NameContainer>
            {iconUrl && !record.cliIngestion ? (
                <Tooltip overlay={typeDisplayName}>
                    <PreviewImage preview={false} src={iconUrl} alt={type || ''} />
                </Tooltip>
            ) : (
                <Icon icon="Plugs" source="phosphor" size="2xl" color="gray" />
            )}
            <DisplayNameContainer>
                {showTooltip ? (
                    <Tooltip
                        title={record.name}
                        color="white"
                        overlayInnerStyle={{ color: colors.gray[1700] }}
                        showArrow={false}
                    >
                        {textElement}
                    </Tooltip>
                ) : (
                    textElement
                )}
                {!iconUrl && typeDisplayName && <SourceTypeText color="gray">{typeDisplayName}</SourceTypeText>}
            </DisplayNameContainer>
            {record.cliIngestion && (
                <Tooltip title="This source is ingested from the command-line interface (CLI)">
                    <div data-testid="ingestion-source-cli-pill">
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
        const finalText = capitalizeFirstLetterOnly(capitalizeMonthsAndDays(cleanedText));
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
            data-testid="schedule"
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
            type: mapEntityTypeToAvatarType(owner.owner.type),
            urn: owner.owner.urn,
        };
    });
    const singleOwner = owners.length === 1 ? owners[0].owner : undefined;

    if (owners.length === 0) return <>-</>;

    return (
        <>
            {singleOwner && (
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
                        type={mapEntityTypeToAvatarType(singleOwner.type)}
                    />
                </Link>
            )}
            {owners.length > 1 && (
                <AvatarStackWithHover avatars={ownerAvatars} showRemainingNumber entityRegistry={entityRegistry} />
            )}
        </>
    );
}

export function wrapOwnerColumnWithHover(content: React.ReactNode, record: any): React.ReactNode {
    const singleOwner = record.owners?.length === 1 ? record.owners[0].owner : undefined;

    if (singleOwner) {
        return (
            <HoverEntityTooltip entity={singleOwner} showArrow={false}>
                <CellHoverWrapper>{content}</CellHoverWrapper>
            </HoverEntityTooltip>
        );
    }

    return content;
}
interface ActionsColumnProps {
    record: any;
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onCancel: (executionUrn: string | undefined, ingestionSourceUrn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
    navigateToRunHistory: (record: IngestionSourceTableData) => void;
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
    onCancel,
    onDelete,
    navigateToRunHistory,
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
    if (record.lastExecUrn) {
        items.push({
            key: '2',
            label: (
                <MenuItem
                    onClick={() => {
                        setFocusExecutionUrn(record.lastExecUrn);
                    }}
                >
                    View Last Run Result
                </MenuItem>
            ),
        });
    }
    if (record.execCount)
        items.push({
            key: '3',
            label: (
                <MenuItem
                    onClick={() => {
                        navigateToRunHistory(record);
                    }}
                >
                    View Run History
                </MenuItem>
            ),
        });
    if (navigator.clipboard)
        items.push({
            key: '4',
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
            key: '5',
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
        key: '6',
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

    const renderRunStopButton = () => {
        if (record.cliIngestion || record.lastExecStatus === EXECUTION_REQUEST_STATUS_LOADING) return null;

        if (record.lastExecStatus === EXECUTION_REQUEST_STATUS_RUNNING) {
            return (
                <Icon
                    icon="Stop"
                    source="phosphor"
                    weight="fill"
                    color="primary"
                    onClick={(e) => {
                        e.stopPropagation();
                        onCancel(record.lastExecUrn, record.urn);
                    }}
                    tooltipText="Stop Execution"
                />
            );
        }
        return (
            <Icon
                icon="Play"
                source="phosphor"
                weight="fill"
                color="violet"
                onClick={(e) => {
                    e.stopPropagation();
                    onExecute(record.urn);
                }}
                tooltipText="Execute"
                data-testid="run-ingestion-source-button"
            />
        );
    };

    return <BaseActionsColumn dropdownItems={items} extraActions={renderRunStopButton()} />;
}
