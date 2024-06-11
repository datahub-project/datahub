import React from 'react';
import styled from 'styled-components';
import moment from 'moment-timezone';
import InfoIcon from '@mui/icons-material/Info';
import SyncOutlinedIcon from '@mui/icons-material/SyncOutlined';
import { Tooltip } from 'antd';
import { REDESIGN_COLORS } from '../../../../constants';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import PlatformIcon from '../../../../../../sharedV2/icons/PlatformIcon';
import { ContentText, LabelText, RelativeTime } from './styledComponents';
import { getPlatformName } from '../../../../utils';

const SyncedAssetContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const DetailsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-direction: column;
    margin: 6px 12px 6px 25px;
`;

const DetailRow = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    flex-wrap: wrap;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    color: #8c7ee0;
    svg {
        font-size: 20px;
    }
`;

const Title = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 14px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.DARK_GREY};
    svg {
        font-size: 18px;
    }
`;

const SyncedAssetSection = () => {
    const { entityData } = useEntityData();

    const platformName = getPlatformName(entityData);
    const lastUpdated = entityData?.lastIngested;
    const isRecentlyUpdated = moment(lastUpdated).isAfter(moment().subtract(1, 'week'));

    if (!lastUpdated) return null;

    return (
        <SyncedAssetContainer>
            <SectionHeader>
                <Title>
                    <SyncOutlinedIcon />
                    Synced Asset
                </Title>
                <Tooltip title="This represents the last time an entity was synchronized." placement="left">
                    <InfoIcon />
                </Tooltip>
            </SectionHeader>
            <DetailsContainer>
                {platformName && (
                    <DetailRow>
                        <LabelText>From: </LabelText>
                        <PlatformIcon platform={entityData?.platform} size={16} />
                        <ContentText>{platformName}</ContentText>
                        <LabelText>on</LabelText>
                        <ContentText>{toLocalDateString(lastUpdated)}</ContentText>
                        <RelativeTime isRecentlyUpdated={isRecentlyUpdated}>
                            {toRelativeTimeString(lastUpdated)}
                        </RelativeTime>
                    </DetailRow>
                )}
            </DetailsContainer>
        </SyncedAssetContainer>
    );
};

export default SyncedAssetSection;
