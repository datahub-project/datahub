import i18next from 'i18next';

export const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

export const INCIDENT_STATE_TO_ACTIVITY = {
    get RAISED() {
        return i18next.t('entity.profile.incident:activity.raised');
    },
    get RESOLVED() {
        return i18next.t('entity.profile.incident:activity.resolved');
    },
};
