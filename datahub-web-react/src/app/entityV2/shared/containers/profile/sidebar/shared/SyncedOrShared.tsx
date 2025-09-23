import { Tooltip } from '@components';
import SwapHorizOutlinedIcon from '@mui/icons-material/SwapHorizOutlined';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import SyncedOrSharedTooltip from '@app/entityV2/shared/containers/profile/sidebar/shared/SyncedOrSharedTooltip';
import {
    ContentText,
    LabelText,
    RelativeTime,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import { ActionType, getRelativeTimeColor } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { toLocalDateString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';

import { DataPlatform, Maybe } from '@types';

const DetailsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-direction: column;
    justify-content: center;
    padding: 5px 8px;
    background-color: ${REDESIGN_COLORS.SECTION_BACKGROUND};
    border-radius: 8px;
`;

const SyncIcon = styled.div`
    color: ${REDESIGN_COLORS.DARK_DIVIDER};
    height: 24px;
    width: 24px;
`;

const DetailRow = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    flex-wrap: wrap;
`;

const SyncedSharedText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    font-weight: 700;
`;

const StyledTooltip = styled(Tooltip)`
    .ant-tooltip-inner {
        border-radius: 4px;
    }
`;

interface Props {
    labelText: string;
    time: number;
    platformName?: string;
    platform?: Maybe<DataPlatform> | undefined;
    type: ActionType;
}

const SyncedOrShared = ({ labelText, time, platformName, platform, type }: Props) => {
    return (
        <DetailsContainer>
            <DetailRow>
                <StyledTooltip
                    showArrow={false}
                    title={<SyncedOrSharedTooltip type={type} />}
                    color={REDESIGN_COLORS.TOOLTIP_BACKGROUND}
                    overlayInnerStyle={{ width: 300, padding: 10 }}
                    placement="bottomLeft"
                >
                    <SyncIcon>
                        <SwapHorizOutlinedIcon />
                    </SyncIcon>
                </StyledTooltip>

                <SyncedSharedText>{labelText} </SyncedSharedText>
                <Tooltip showArrow={false} title={toLocalDateString(time)}>
                    <RelativeTime relativeTimeColor={getRelativeTimeColor(time)}>
                        {toRelativeTimeString(time)}
                    </RelativeTime>
                </Tooltip>
                {!!platform && (
                    <>
                        <LabelText>from</LabelText>
                        <PlatformIcon platform={platform} size={16} />
                        <ContentText color={REDESIGN_COLORS.BODY_TEXT}>{platformName}</ContentText>
                    </>
                )}
            </DetailRow>
        </DetailsContainer>
    );
};

export default SyncedOrShared;
