import { green, orange, red } from '@ant-design/colors';
import moment from 'moment-timezone';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';

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

