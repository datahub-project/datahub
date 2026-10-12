import { CellHoverWrapper, Icon, Pill, Text, Tooltip } from '@components';
import { Play } from '@phosphor-icons/react/dist/csr/Play';
import { Plugs } from '@phosphor-icons/react/dist/csr/Plugs';
import { Stop } from '@phosphor-icons/react/dist/csr/Stop';
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components/macro';

import { ItemType } from '@components/components/Menu/types';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { EXECUTION_REQUEST_STATUS_LOADING, EXECUTION_REQUEST_STATUS_RUNNING } from '@app/ingestV2/executions/constants';
import BaseActionsColumn from '@app/ingestV2/shared/components/columns/BaseActionsColumn';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';
import { IngestionSourceTableData } from '@app/ingestV2/source/types';
import { capitalizeMonthsAndDays, formatTimezone } from '@app/ingestV2/source/utils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { capitalizeFirstLetter, capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { OwnerAvatarGroup } from '@app/sharedV2/owners/OwnerAvatarGroup';
import { cronToString, removeTimePrefix } from '@utils/cronstrue';

import { Owner } from '@types';

const PreviewImage = styled.img`
    max-height: 20px;
    width: auto;
    max-width: 28px;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

const TextContainer = styled(Text)<{ $shouldUnderline?: boolean }>`
    color: ${(props) => props.theme.colors.textSecondary};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    ${(props) =>
        props.$shouldUnderline &&
        `
            :hover {
                text-decoration: underline;
            }
        `}
`;

const SourceNameText = styled.div<{ $shouldUnderline?: boolean }>`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    line-height: 1.3;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    word-break: break-word;
    white-space: normal;
    text-overflow: unset;

    ${(props) =>
        props.$shouldUnderline &&
        `
            :hover {
                text-decoration: underline;
            }
        `}
`;

const SourceTypeText = styled(Text)`
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
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
    const { t } = useTranslation('ingestion');
    const theme = useTheme();
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
                    <PreviewImage src={iconUrl} alt={type || ''} />
                </Tooltip>
            ) : (
                <Icon icon={Plugs} size="2xl" color="icon" />
            )}
            <DisplayNameContainer>
                {showTooltip ? (
                    <Tooltip
                        title={record.name}
                        overlayInnerStyle={{ color: theme.colors.textSecondary }}
                        showArrow={false}
                    >
                        {textElement}
                    </Tooltip>
                ) : (
                    textElement
                )}
                {!iconUrl && typeDisplayName && (
                    <SourceTypeText color="textSecondary">{typeDisplayName}</SourceTypeText>
                )}
            </DisplayNameContainer>
            {record.cliIngestion && (
                <Tooltip title={t('source.cliTooltip')}>
                    <div data-testid="ingestion-source-cli-pill">
                        <Pill label="CLI" color="blue" size="xs" />
                    </div>
                </Tooltip>
            )}
        </NameContainer>
    );
}

/**
 * The human-readable schedule shown in source lists ("Every day at 12:00 am (UTC)").
 * Returns null when the cron doesn't parse so callers can render their own
 * "invalid" copy; an empty schedule yields an empty string.
 */
export function formatScheduleText(schedule: string, timezone?: string): string | null {
    try {
        const text = schedule && `${cronToString(schedule).toLowerCase()} (${formatTimezone(timezone)})`;
        const cleanedText = removeTimePrefix(text);
        return capitalizeFirstLetterOnly(capitalizeMonthsAndDays(cleanedText)) ?? '-';
    } catch (e) {
        console.debug('Error parsing cron schedule', e);
        return null;
    }
}

export function ScheduleColumn({ schedule, timezone }: { schedule: string; timezone?: string }) {
    const { t } = useTranslation('ingestion');
    const theme = useTheme();
    const scheduleText = formatScheduleText(schedule, timezone) ?? t('source.invalidCron');
    return (
        <Tooltip title={scheduleText} overlayInnerStyle={{ color: theme.colors.textSecondary }} showArrow={false}>
            <TextContainer data-testid="schedule">{scheduleText || '-'}</TextContainer>
        </Tooltip>
    );
}

export function OwnerColumn({ owners, entityRegistry }: { owners: Owner[]; entityRegistry: EntityRegistry }) {
    if (owners.length === 0) return <>-</>;

    return <OwnerAvatarGroup owners={owners} entityRegistry={entityRegistry} />;
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
    record: IngestionSourceTableData;
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onCancel: (executionUrn: string | undefined, ingestionSourceUrn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
    navigateToRunHistory: (record: IngestionSourceTableData) => void;
}

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
    const { t } = useTranslation('ingestion');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const items: ItemType[] = [];

    if (!record.cliIngestion)
        items.push({
            type: 'item',
            key: 'edit',
            title: tc('edit'),
            onClick: () => {
                onEdit(record.urn);
            },
        });
    else
        items.push({
            type: 'item',
            key: 'view',
            title: tc('view'),
            onClick: () => {
                onView(record.urn);
            },
        });
    if (record.lastExecUrn) {
        items.push({
            type: 'item',
            key: 'view-last-run',
            title: t('source.viewLastRunResult'),
            onClick: () => {
                setFocusExecutionUrn(record.lastExecUrn || '');
            },
        });
    }
    if (record.execCount)
        items.push({
            type: 'item',
            key: 'run-history',
            title: t('source.viewRunHistory'),
            onClick: () => {
                navigateToRunHistory(record);
            },
        });
    if (navigator.clipboard)
        items.push({
            type: 'item',
            key: 'copy-urn',
            title: t('source.copyUrn'),
            onClick: () => {
                navigator.clipboard.writeText(record.urn);
            },
        });
    if (record.lastExecStatus === EXECUTION_REQUEST_STATUS_RUNNING)
        items.push({
            type: 'item',
            key: 'details',
            title: tl('details'),
            onClick: () => {
                setFocusExecutionUrn(record.lastExecUrn || '');
            },
        });
    items.push({
        type: 'item',
        key: 'delete',
        title: tc('delete'),
        danger: true,
        onClick: () => onDelete(record.urn),
    });

    const renderRunStopButton = () => {
        if (record.cliIngestion || record.lastExecStatus === EXECUTION_REQUEST_STATUS_LOADING) return null;

        if (record.lastExecStatus === EXECUTION_REQUEST_STATUS_RUNNING) {
            return (
                <Icon
                    icon={Stop}
                    size="lg"
                    weight="fill"
                    color="iconBrand"
                    onClick={(e) => {
                        e.stopPropagation();
                        onCancel(record.lastExecUrn, record.urn);
                    }}
                    tooltipText={t('source.stopExecution')}
                />
            );
        }
        return (
            <Icon
                icon={Play}
                size="lg"
                weight="fill"
                color="iconBrand"
                onClick={(e) => {
                    e.stopPropagation();
                    onExecute(record.urn);
                }}
                tooltipText={t('source.execute')}
                data-testid="run-ingestion-source-button"
            />
        );
    };

    return <BaseActionsColumn dropdownItems={items} extraActions={renderRunStopButton()} />;
}
