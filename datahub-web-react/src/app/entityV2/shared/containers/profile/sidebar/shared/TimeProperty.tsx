import React from 'react';
import { Tooltip } from 'antd';
import styled from 'styled-components';
import { LabelText } from './styledComponents';
import { REDESIGN_COLORS } from '../../../../constants';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';

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
}

const TimeProperty = ({ labelText, time }: Props) => {
    return (
        <PropertyContainer>
            <LabelText>{labelText}</LabelText>
            {!!time && (
                <Tooltip title={toLocalDateString(time)}>
                    <UpdatedTime>{toRelativeTimeString(time)}</UpdatedTime>
                </Tooltip>
            )}
        </PropertyContainer>
    );
};

export default TimeProperty;
