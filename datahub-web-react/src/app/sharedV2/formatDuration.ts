import i18next from 'i18next';

export const formatDuration = (durationMs: number): string => {
    if (!durationMs) return i18next.t('common.labels:none');

    const seconds = durationMs / 1000;

    if (seconds < 60) {
        /* untranslated-text -- locale-invariant unit symbol */
        return `${seconds.toFixed(1)} s`;
    }

    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.round(seconds % 60);

    if (minutes < 60) {
        /* untranslated-text -- locale-invariant unit symbols */
        return `${minutes} min ${remainingSeconds} s`;
    }

    const hours = Math.floor(minutes / 60);
    const remainingMinutes = Math.round(minutes % 60);

    /* untranslated-text -- locale-invariant unit symbols */
    return `${hours} hr ${remainingMinutes} min`;
};
