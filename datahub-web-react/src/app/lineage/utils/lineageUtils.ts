import dayjs from 'dayjs';

export const getDefaultLineageStartTime = () => {
    return Math.round(dayjs().subtract(14, 'day').valueOf() / 60000) * 60000;
};

export const getDefaultLineageEndTime = () => {
    return Math.round(dayjs().valueOf() / 60000) * 60000;
};
