export function translateDisplayNames(t: any, displayName: string | null | undefined): string {
    if (!displayName) return '';
    const displayNameFormatted = displayName
        .trim()
        .replaceAll(' ', '')
        .replaceAll(/[^a-zA-Z\s]/g, '')
        .toLowerCase();

    const FIELD_TO_DISPLAY_NAMES = {
        groups: t('common.groups'),
        users: t('common.users'),
    };

    const entries = Object.entries(FIELD_TO_DISPLAY_NAMES);
    const entry = entries.find(([key]) => key === displayNameFormatted);
    return entry ? entry[1] : displayName;
}
