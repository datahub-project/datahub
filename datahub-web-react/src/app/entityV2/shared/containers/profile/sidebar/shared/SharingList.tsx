import React from 'react';
import moment from 'moment-timezone';
import InfoIcon from '@mui/icons-material/Info';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import AcrylIcon from '../../../../../../../images/acryl-logo.svg?react';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';
import { ShareResult } from '../../../../../../../types.generated';
import { ContentText, InstanceIcon, LabelText, RelativeTime } from './styledComponents';
import { REDESIGN_COLORS } from '../../../../constants';

import ShareIcon from '../../../../../../../images/share-icon-custom.svg?react';

const StyledShareIcon = styled(ShareIcon)`
    height: 18px;
    width: 18px;
    path {
        stroke: ${REDESIGN_COLORS.BODY_TEXT};
    }
`;

const ResultsContainer = styled.div`
    & > div {
        padding-top: 12px;
        &:not(:last-child) {
            padding-bottom: 12px;
            border-bottom: 1px dashed;
            border-color: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
        }
    }
`;

const DetailsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-direction: column;
`;

const DetailRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const UpdatedRow = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    margin-left: 24px;
`;

const Content = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
`;

const Icon = styled.div`
    color: #8c7ee0;
    svg {
        font-size: 20px;
    }
`;

type Props = {
    resultsList: ShareResult[];
};

const SharingList = ({ resultsList }: Props) => {
    return (
        <ResultsContainer>
            {resultsList.map((result) => {
                const name = result.destination.details.name || result.destination.urn;
                const lastSuccessTime = result.lastSuccess?.time || 0;
                const isRecentlyUpdated = moment(lastSuccessTime).isAfter(moment().subtract(1, 'week'));

                return (
                    <DetailsContainer key={name}>
                        <DetailRow>
                            <Content>
                                <StyledShareIcon />
                                <LabelText>To: </LabelText>
                                <InstanceIcon>
                                    <AcrylIcon />
                                </InstanceIcon>
                                <ContentText>{name}</ContentText>
                            </Content>
                            <Icon>
                                <Tooltip title="This represents the last time an entity was shared." placement="left">
                                    <InfoIcon />
                                </Tooltip>
                            </Icon>
                        </DetailRow>
                        <UpdatedRow>
                            <LabelText>Date Updated: </LabelText>
                            <ContentText>{toLocalDateString(lastSuccessTime)}</ContentText>
                            <RelativeTime isRecentlyUpdated={isRecentlyUpdated}>
                                {toRelativeTimeString(lastSuccessTime)}
                            </RelativeTime>
                        </UpdatedRow>
                    </DetailsContainer>
                );
            })}
        </ResultsContainer>
    );
};

export default SharingList;
