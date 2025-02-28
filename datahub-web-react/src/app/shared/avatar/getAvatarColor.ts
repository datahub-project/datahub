import { lineColors } from '../../analyticsDashboard/components/lineColors';
import { ANTD_GRAY } from '../../entity/shared/constants';

export function hashString(str: string) {
    let hash = 0;
    if (str.length === 0) {
        return hash;
    }
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        // eslint-disable-next-line
        hash = (hash << 5) - hash + char;
        // eslint-disable-next-line
        hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash);
}

export default function getAvatarColor(name?: string) {
    if (!name) {
        return ANTD_GRAY[7];
    }
    return lineColors[hashString(name) % lineColors.length];
}
