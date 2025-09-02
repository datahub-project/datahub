import React from 'react';

import Properties from '@app/entityV2/summary/properties/Properties';
import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import { StyledDivider } from '@app/entityV2/summary/styledComponents';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

/**
 * The wrapper to show/hide divider with the whole section
 */
export default function PropertiesWithDividerWrapper() {
    const { editable } = useAssetPropertiesContext();
    const { summaryElements } = usePageTemplateContext();

    // Do not show properties section at all when there are no properties
    // and the current user has no permissions to manage properties
    if (!summaryElements?.length && !editable) return null;

    return (
        <>
            <Properties />
            <StyledDivider />
        </>
    );
}
