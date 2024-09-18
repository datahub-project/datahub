/**
 * Automation Application Type Options
 */

export enum TraversalTypes {
    UPSTREAM = 'upstream',
    DOWNSTREAM = 'downstream',
    PARENT = 'parent',
    CHILD = 'child',
}

export const TRAVERSAL_OPTIONS = {
    lineage: [
        {
            key: TraversalTypes.UPSTREAM,
            name: 'Upstream',
            description: 'Propagate to upstream lineage',
        },
        {
            key: TraversalTypes.DOWNSTREAM,
            name: 'Downstream',
            description: 'Propagate to downstream lineage',
        },
    ],
    hierarchy: [
        {
            key: TraversalTypes.PARENT,
            name: 'Include Parents',
            description: 'Propagate to parent hierarchy',
        },
        {
            key: TraversalTypes.CHILD,
            name: 'Include Children',
            description: 'Propagate to child hierarchy',
        },
    ],
};
