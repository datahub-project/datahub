import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';

dayjs.extend(LocalizedFormat);

export function formatTimestamp(timestamp: number, format: string) {
    return dayjs(timestamp).format(format);
}
