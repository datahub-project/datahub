import dayjs from 'dayjs';

export function formatTimestamp(timestamp: number, format: string) {
    return dayjs(timestamp).format(format);
}
