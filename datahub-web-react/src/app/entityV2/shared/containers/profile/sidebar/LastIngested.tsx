import { green, orange, red } from '@ant-design/colors';
import { QuestionCircleOutlined } from '@ant-design/icons';
import { Image } from 'antd';
import moment from 'moment-timezone';
import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const StyledDot = styled.div<{ color: string }>`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 50%;
    background-color: ${(props) => props.color};
    width: 5px;
    height: 5px;
    margin-right: 5px;
    vertical-align: middle;
`;

const TooltipSection = styled.div`
    align-items: center;
    display: flex;
    margin-bottom: 5px;
`;

export function getLastIngestedColor(lastIngested: number) {
    const lastIngestedDate = moment(lastIngested);
    if (lastIngestedDate.isAfter(moment().subtract(1, 'week'))) {
        return green[5];
    }
    if (lastIngestedDate.isAfter(moment().subtract(1, 'month'))) {
        return orange[5];
    }
    return red[5];
}

