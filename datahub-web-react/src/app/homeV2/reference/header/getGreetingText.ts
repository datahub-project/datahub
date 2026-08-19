import i18next from 'i18next';

export const getGreetingText = () => {
    const currentHour = new Date().getHours(); // gets the current hour (0-23)
    if (currentHour < 12) {
        return i18next.t('home.v2:greeting.morning');
    }
    if (currentHour < 17) {
        return i18next.t('home.v2:greeting.afternoon');
    }
    return i18next.t('home.v2:greeting.evening');
};
