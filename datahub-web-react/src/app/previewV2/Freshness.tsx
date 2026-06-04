import { Popover } from '@components';
import UpdateOutlinedIcon from '@mui/icons-material/UpdateOutlined';
import i18next from 'i18next';
import React from 'react';
import styled from 'styled-components';

import { getLastIngestedColor } from '@app/entity/shared/containers/profile/sidebar/LastIngested';
import { toLocalDateString, toRelativeTimeString } from '@app/shared/time/timeUtils';

const LastUpdatedContainer = styled.div<{ color: string }>`
    align-items: center;
    color: ${(props) => props.theme.colors.textTertiary};
    display: flex;
    flex-direction: row;
    gap: 5px;
    svg {
        font-size: 16px;
        color: ${(props) => props.color};
    }
`;

const PopoverContent = styled.div`
    align-items: center;
    display: flex;
    color: ${(props) => props.theme.colors.textSecondary};
`;

type Props = {
    time?: number; // Milliseconds
    showDate?: boolean;
    timeProperty?: 'lastModified' | 'lastRefreshed' | 'lastUpdated';
};

// Lazy getters so i18next.t is invoked at access time (after i18n is initialized),
// not at module import time.
const descriptors = {
    lastModified: {
        get sectionTitle() {
            return i18next.t('entity.preview:freshness.lastModified.sectionTitle');
        },
        get tooltip() {
            return i18next.t('entity.preview:freshness.lastModified.tooltip');
        },
    },
    lastRefreshed: {
        get sectionTitle() {
            return i18next.t('entity.preview:freshness.lastRefreshed.sectionTitle');
        },
        get tooltip() {
            return i18next.t('entity.preview:freshness.lastRefreshed.tooltip');
        },
    },
    lastUpdated: {
        get sectionTitle() {
            return i18next.t('entity.preview:freshness.lastUpdated.sectionTitle');
        },
        get tooltip() {
            return i18next.t('entity.preview:freshness.lastUpdated.tooltip');
        },
    },
};

const Freshness = ({ time, timeProperty, showDate = true }: Props) => {
    const lastUpdatedAgo = toRelativeTimeString(time);

    if (!time || time === 0) return null;

    let updateType;
    switch (timeProperty) {
        case 'lastModified':
            updateType = descriptors.lastModified.tooltip;
            break;
        case 'lastRefreshed':
            updateType = descriptors.lastRefreshed.tooltip;
            break;
        default: // default to "lastUpdated"
            updateType = descriptors.lastUpdated.tooltip;
            break;
    }

    return (
        <Popover
            content={
                /* eslint-disable i18next/no-literal-string -- (untranslated-text) dynamic relative-time join; `updateType` is already translated and `lastUpdatedAgo` is a localized relative-time string with language-varying word order */
                <PopoverContent>{`${updateType} ${lastUpdatedAgo}`}</PopoverContent>
                /* eslint-enable i18next/no-literal-string */
            }
            placement="bottom"
            showArrow={false}
        >
            <LastUpdatedContainer color={getLastIngestedColor(time)}>
                <UpdateOutlinedIcon /> {showDate && toLocalDateString(time)}
            </LastUpdatedContainer>
        </Popover>
    );
};

export default Freshness;
