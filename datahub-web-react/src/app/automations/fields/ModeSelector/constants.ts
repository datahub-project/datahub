/**
 * Automation Mode Type Options
 */

export enum ModeTypes {
    POLICY_TAGS = 'policy_tags',
    LABELS = 'labels',
}

export const MODE_OPTIONS = [
    {
        key: ModeTypes.POLICY_TAGS,
        name: 'Policy Tags',
        description: 'Only apply to Columns',
    },
    {
        key: ModeTypes.LABELS,
        name: 'Labels',
        description: 'Only apply to Tables',
    },
];
