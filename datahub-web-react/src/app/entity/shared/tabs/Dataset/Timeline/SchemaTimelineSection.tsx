import React from 'react';

import { useEntityData } from '../../../EntityContext';
import { useGetTimelineQuery } from '../../../../../../graphql/timeline.generated';
import { ChangeCategoryType } from '../../../../../../types.generated';

export const SchemaTimelineSection = () => {
    const { urn } = useEntityData();

    const timelineResult = useGetTimelineQuery({
        // also pass in the changeCategories
        variables: { input: { urn, changeCategories: [ChangeCategoryType.TechnicalSchema] } },
    });

    // Dynamically load the schema + editable schema information.
    return <>{JSON.stringify(timelineResult.data?.getTimeline?.changeTransactions, undefined, 2)}</>;
};
