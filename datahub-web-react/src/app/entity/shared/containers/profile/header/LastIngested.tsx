import { green, orange, red } from '@ant-design/colors';
import { Popover } from 'antd';
import styled from 'styled-components/macro';
import moment from 'moment-timezone';
import React from 'react';
import { toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../../constants';

const StyledDot = styled.div<{ color: string }>`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 50%;
    background-color: ${(props) => props.color};
    width: 10px;
    height: 10px;
    margin-left: 15px;
    vertical-align: middle;
`;

const PopoverContentWrapper = styled.div`
    align-items: center;
    display: flex;

    ${StyledDot} {
        margin: 0 8px 0 0;
    }
`;

interface Props {
    lastIngested: number;
    displayedEntityType: string;
}

function getLastIngestedColor(lastIngested: number) {
    const lastIngestedDate = moment(lastIngested);
    if (lastIngestedDate.isAfter(moment().subtract(1, 'week'))) {
        return green[5];
    }
    if (lastIngestedDate.isAfter(moment().subtract(1, 'month'))) {
        return orange[5];
    }
    return red[5];
}

function LastIngested({ lastIngested, displayedEntityType }: Props) {
    const lastIngestedColor = getLastIngestedColor(lastIngested);

    return (
        <Popover
            placement="left"
            content={
                <PopoverContentWrapper>
                    <StyledDot color={lastIngestedColor} />
                    This {displayedEntityType.toLocaleLowerCase()} was last synchronized&nbsp;
                    <b>{toRelativeTimeString(lastIngested)}</b>
                </PopoverContentWrapper>
            }
        >
            <StyledDot color={lastIngestedColor} />
        </Popover>
    );
}

export default LastIngested;
