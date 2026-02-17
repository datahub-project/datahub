import { Tooltip } from '@components';
import SwapHorizOutlinedIcon from '@mui/icons-material/SwapHorizOutlined';
import { Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

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
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-radius: 8px;
`;

const SyncIcon = styled.div`
    color: ${(props) => props.theme.colors.border};
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
    color: ${(props) => props.theme.colors.text};
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
    const theme = useTheme();
    return (
        <DetailsContainer>
            <DetailRow>
                <StyledTooltip
                    showArrow={false}
                    title={<SyncedOrSharedTooltip type={type} />}
                    color="#272D48"
                    overlayInnerStyle={{ width: 300, padding: 10 }}
                    placement="bottomLeft"
                >
                    <SyncIcon>
                        <SwapHorizOutlinedIcon />
                    </SyncIcon>
                </StyledTooltip>

                <SyncedSharedText>{labelText} </SyncedSharedText>
                <Tooltip showArrow={false} title={toLocalDateString(time)}>
                    <RelativeTime relativeTimeColor={getRelativeTimeColor(time, theme)}>
                        {toRelativeTimeString(time)}
                    </RelativeTime>
                </Tooltip>
                {!!platform && (
                    <>
                        <LabelText>from</LabelText>
                        <PlatformIcon platform={platform} size={16} />
                        <ContentText color={theme.colors.textSecondary}>{platformName}</ContentText>
                    </>
                )}
            </DetailRow>
        </DetailsContainer>
    );
};

export default SyncedOrShared;
