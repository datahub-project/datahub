import React from 'react';
import styled from 'styled-components';
import moment from 'moment-timezone';
import SwapHorizOutlinedIcon from '@mui/icons-material/SwapHorizOutlined';
import { Tooltip } from 'antd';
import { REDESIGN_COLORS } from '../../../../constants';
import AcrylIcon from '../../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../../images/share-icon-custom.svg?react';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';
import PlatformIcon from '../../../../../../sharedV2/icons/PlatformIcon';
import { ContentText, InstanceIcon, LabelText, RelativeTime } from './styledComponents';
import { DataPlatform, Maybe } from '../../../../../../../types.generated';
import { ACRYL_PLATFORM } from './utils';

const DetailsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-direction: column;
    align-items: end;
    justify-content: center;
    padding-right: 4px;
    background-color: ${REDESIGN_COLORS.SECTION_BACKGROUND};
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
    padding: 5px 2px;
`;

const StyledShareIcon = styled(ShareIcon)`
    height: 24px;
    width: 24px;
`;

interface Props {
    labelText: string;
    time: number;
    platformName?: string;
    platform?: Maybe<DataPlatform> | undefined;
    instanceName?: string;
}

const SyncedOrShared = ({ labelText, time, platformName, platform, instanceName }: Props) => {
    const isRecentlyUpdated = moment(time).isAfter(moment().subtract(1, 'week'));

    return (
        <DetailsContainer>
            <DetailRow>
                <SyncIcon>{platformName === ACRYL_PLATFORM ? <StyledShareIcon /> : <SwapHorizOutlinedIcon />}</SyncIcon>
                <LabelText>{labelText} </LabelText>
                <Tooltip title={toLocalDateString(time)}>
                    <RelativeTime isRecentlyUpdated={isRecentlyUpdated}>{toRelativeTimeString(time)}</RelativeTime>
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
