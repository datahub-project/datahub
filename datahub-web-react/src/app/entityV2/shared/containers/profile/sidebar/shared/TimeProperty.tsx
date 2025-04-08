import { Tooltip } from '@components';
import moment from 'moment';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { LabelText } from '@app/entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import { toLocalDateString, toRelativeTimeString } from '@app/shared/time/timeUtils';

const PropertyContainer = styled.div`
    display: flex;
    gap: 5px;
    margin-bottom: 6px;
`;

const UpdatedTime = styled.span`
    font-weight: 700;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

interface Props {
    labelText: string;
    time?: number | null;
    titleTip?: React.ReactNode;
}

const TimeProperty = ({ labelText, time, titleTip }: Props) => {
    const timeFormatted = moment(time).format('hh:mm A');
    return (
        <PropertyContainer>
            <Tooltip showArrow={false} title={titleTip}>
                <LabelText>{labelText}</LabelText>
            </Tooltip>
            {!!time && (
                <Tooltip showArrow={false} title={`${toLocalDateString(time)} at ${timeFormatted}`}>
                    <UpdatedTime>{toRelativeTimeString(time)}</UpdatedTime>
                </Tooltip>
            )}
        </PropertyContainer>
    );
};

export default TimeProperty;
