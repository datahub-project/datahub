import React, { useState } from 'react';
import { EntityType } from '../shared/EntityTypeUtil';

interface Props {
    type: EntityType; // type of entity associated with the urn. may be able to provide a util to infer this from the urn. we use this to fetch the results.
    path: string; // the path to fetch results for.
}

/**
 * Needs implemented!
 */
export const BrowseList: React.FC<Props> = ({ type, path }) => {
    return <div>Needs Implemented</div>;
};
