import { CronSchedule, Monitor } from '../../../../../../../../../types.generated';

export const toReadableLocalDateTimeString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        timeZoneName: 'short',
    });
};

export const tryGetScheduleFromMonitor = (monitor?: Monitor): CronSchedule | undefined => {
    return monitor?.info?.assertionMonitor?.assertions?.length
        ? monitor?.info?.assertionMonitor?.assertions[0]?.schedule
        : undefined;
};
