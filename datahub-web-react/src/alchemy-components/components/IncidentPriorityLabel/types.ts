export interface IncidentPriorityLabelProps {
    priority: string;
    /** Optional label override. Defaults to the capitalised `priority`; pass a translated
     *  priority name (e.g. from `entity.profile.incident:priority.*`) to keep it localised. */
    title?: string;
    style?: React.CSSProperties;
}
