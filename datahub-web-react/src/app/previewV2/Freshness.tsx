import React from 'react';

import { Popover } from 'antd';
import UpdateOutlinedIcon from '@mui/icons-material/UpdateOutlined';
import styled from 'styled-components';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';

import { ANTD_GRAY } from '../entity/shared/constants';
import { getLastIngestedColor } from '../entity/shared/containers/profile/sidebar/LastIngested';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

dayjs.extend(localizedFormat);

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
    timeProperty?: string;
};

export const getFreshnessTitle = (property: string | undefined) => {
    switch (property) {
        case 'lastModified':
            return 'Changed';
        case 'lastRefreshed':
            return 'Freshness';
        default:
            return 'Changed';
    }
}

const Freshness = ({ time, timeProperty, showDate = true }: Props) => {
    const lastUpdatedAgo = dayjs(time).fromNow();

    if (!time || time === 0) return null;

    let updateType;
    switch (timeProperty) {
        case 'lastModified':
            updateType = 'Last modified';
            break;
        case 'lastRefreshed':
            updateType = 'Last refreshed';
            break;
        default: // default to "last updated"
            updateType = 'Last updated';
            break;
    }

    return (
        <Popover
            content={<PopoverContent>{`${updateType} ${lastUpdatedAgo}`}</PopoverContent>}
            placement="bottom"
            showArrow={false}
        >
            <LastUpdatedContainer color={getLastIngestedColor(time)}>
                <UpdateOutlinedIcon /> {showDate && dayjs(time).format('L')}
            </LastUpdatedContainer>
        </Popover>
    );
};

export default Freshness;
