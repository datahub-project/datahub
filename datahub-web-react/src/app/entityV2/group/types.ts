// Enum values double as route paths and deep-link targets, so they stay stable (English) and must not
// be translated. User-facing tab labels are resolved separately via i18n in GroupProfile (see getTabs).
export enum TabType {
    Assets = 'Owner Of',
    Members = 'Members',
}
