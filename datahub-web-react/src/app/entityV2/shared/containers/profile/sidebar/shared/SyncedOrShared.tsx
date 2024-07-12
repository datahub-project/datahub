import React from 'react';
import styled from 'styled-components';
import SwapHorizOutlinedIcon from '@mui/icons-material/SwapHorizOutlined';
import { Tooltip, Typography } from 'antd';
import { REDESIGN_COLORS } from '../../../../constants';
import AcrylIcon from '../../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../../images/share-icon-custom.svg?react';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';
import PlatformIcon from '../../../../../../sharedV2/icons/PlatformIcon';
import { ContentText, InstanceIcon, LabelText, RelativeTime } from './styledComponents';
import { DataPlatform, Maybe } from '../../../../../../../types.generated';
import { ACRYL_PLATFORM, getRelativeTimeColor } from './utils';

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

const StyledShareIcon = styled(ShareIcon)`
    height: 24px;
    width: 24px;
`;

const SyncedSharedText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    font-weight: 700;
`;

interface Props {
    labelText: string;
    time: number;
    platformName?: string;
    platform?: Maybe<DataPlatform> | undefined;
    instanceName?: string;
}

const SyncedOrShared = ({ labelText, time, platformName, platform, instanceName }: Props) => {
    return (
        <DetailsContainer>
            <DetailRow>
                <SyncIcon>{platformName === ACRYL_PLATFORM ? <StyledShareIcon /> : <SwapHorizOutlinedIcon />}</SyncIcon>
                <SyncedSharedText>{labelText} </SyncedSharedText>
                <Tooltip title={toLocalDateString(time)}>
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
                {platformName === ACRYL_PLATFORM && !!instanceName && (
                    <>
                        <LabelText>from</LabelText>
                        <InstanceIcon>
                            <AcrylIcon />
                        </InstanceIcon>
                        <ContentText color={REDESIGN_COLORS.BODY_TEXT}>
                            {instanceName.replace('https://', '')}
                        </ContentText>
                    </>
                )}
            </DetailRow>
        </DetailsContainer>
    );
};

export default SyncedOrShared;
