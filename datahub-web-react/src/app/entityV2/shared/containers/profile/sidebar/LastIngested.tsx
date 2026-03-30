import { green, orange, red } from '@ant-design/colors';

import dayjs from '@utils/dayjs';

export function getLastIngestedColor(lastIngested: number) {
    const lastIngestedDate = dayjs(lastIngested);
    if (lastIngestedDate.isAfter(dayjs().subtract(1, 'week'))) {
        return green[5];
    }
    if (lastIngestedDate.isAfter(dayjs().subtract(1, 'month'))) {
        return orange[5];
    }
    return red[5];
}
