import React from 'react';
import { EntityType } from '../../types.generated';

interface Props {
    type: EntityType; // type of entity associated with the urn. may be able to provide a util to infer this from the urn. we use this to fetch the results.
    path: string; // the path to fetch results for.
}

/**
 * Needs implemented!
 */
export const BrowseList: React.FC<Props> = (_: Props) => {
    return <div>Needs Implemented</div>;
};
