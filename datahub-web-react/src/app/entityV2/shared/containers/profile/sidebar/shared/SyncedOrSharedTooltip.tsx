import { Pill, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ActionType, RelativeTimeStatus } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import HoverCardHeader from '@app/sharedV2/hoverCard/HoverCardHeader';
import HoverCardSection from '@app/sharedV2/hoverCard/HoverCardSection';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { capitalizeFirstLetterOnly } from '@app/sharedV2/textUtil';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/sharedV2/time/timeUtils';

import { DataPlatform, Maybe } from '@types';

const LEGEND: { color: RelativeTimeStatus; labelKey: string }[] = [
    { color: 'green', labelKey: 'sidebar.syncedOrShared.hoverCard.legendPastWeek' },
    { color: 'yellow', labelKey: 'sidebar.syncedOrShared.hoverCard.legendPastMonth' },
    { color: 'red', labelKey: 'sidebar.syncedOrShared.hoverCard.legendOlder' },
];

// Same shell as `EntityHoverCard` so this reads as one of the platform's hover cards.
const Card = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: 320px;
    max-width: calc(100vw - 32px);
`;

const Sections = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const LegendRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

type Props = {
    type: ActionType;
    time: number;
    platformName?: string;
    platform?: Maybe<DataPlatform>;
};

export default function SyncedOrSharedTooltip({ type, time, platformName, platform }: Props) {
    const { t } = useTranslation('entity.shared.containers');
    const isSync = type === ActionType.SYNC;

    return (
        <Card>
            <HoverCardHeader
                title={
                    isSync
                        ? t('sidebar.syncedOrShared.hoverCard.syncTitle')
                        : t('sidebar.syncedOrShared.hoverCard.shareTitle')
                }
                icon={platform ? <PlatformIcon platform={platform} size={24} /> : undefined}
                typeName={platformName}
            />
            <Sections>
                <HoverCardSection
                    title={
                        isSync
                            ? t('sidebar.syncedOrShared.hoverCard.lastSynced')
                            : t('sidebar.syncedOrShared.hoverCard.lastShared')
                    }
                >
                    <Text>
                        {t('sidebar.syncedOrShared.hoverCard.timeValue', {
                            relativeTime: capitalizeFirstLetterOnly(toRelativeTimeString(time)),
                            dateTime: toLocalDateTimeString(time),
                        })}
                    </Text>
                </HoverCardSection>
                {isSync && (
                    <HoverCardSection title={t('sidebar.syncedOrShared.hoverCard.aboutTitle')}>
                        <Text>{t('sidebar.syncedOrShared.hoverCard.metadataOnlyNote')}</Text>
                    </HoverCardSection>
                )}
                <HoverCardSection title={t('sidebar.syncedOrShared.hoverCard.freshnessTitle')}>
                    <LegendRow>
                        {LEGEND.map(({ color, labelKey }) => (
                            <Pill key={color} label={t(labelKey)} variant="filled" color={color} size="sm" />
                        ))}
                    </LegendRow>
                </HoverCardSection>
            </Sections>
        </Card>
    );
}
