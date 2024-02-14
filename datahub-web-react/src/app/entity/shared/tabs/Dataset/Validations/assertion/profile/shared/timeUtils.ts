export const toReadableLocalDateTimeString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short',
    });
};
