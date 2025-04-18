import React from 'react';

import { Popover } from '@components';
import UpdateOutlinedIcon from '@mui/icons-material/UpdateOutlined';
import styled from 'styled-components';

import { ANTD_GRAY } from '../entity/shared/constants';
import { getLastIngestedColor } from '../entity/shared/containers/profile/sidebar/LastIngested';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

import { toLocalDateString, toRelativeTimeString } from '../shared/time/timeUtils';

const LastUpdatedContainer = styled.div<{ color: string }>`
    align-items: center;
    color: ${ANTD_GRAY[7]};
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
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

type Props = {
    time?: number; // Milliseconds
    showDate?: boolean;
    timeProperty?: 'lastModified' | 'lastRefreshed' | 'lastUpdated';
};

const descriptors = {
    lastModified: {
        sectionTitle: 'Last Modified',
        tooltip: 'Last modified',
    },
    lastRefreshed: {
        sectionTitle: 'Data Last Refreshed',
        tooltip: 'Data last refreshed',
    },
    lastUpdated: {
        sectionTitle: 'Last Updated',
        tooltip: 'Last updated',
    },
};

export const getFreshnessTitle = (property: string | undefined) => {
    switch (property) {
        case 'lastModified':
            return descriptors.lastModified.sectionTitle;
        case 'lastRefreshed':
            return descriptors.lastRefreshed.sectionTitle;
        default: // default to "lastUpdated"
            return descriptors.lastUpdated.sectionTitle;
    }
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
            content={<PopoverContent>{`${updateType} ${lastUpdatedAgo}`}</PopoverContent>}
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
