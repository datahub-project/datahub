import React from 'react';

import Properties from '@app/entityV2/summary/properties/Properties';
import { StyledDivider } from '@app/entityV2/summary/styledComponents';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

/**
 * The wrapper to show/hide divider with the whole section
 */
export default function PropertiesWithDividerWrapper() {
    const { summaryElements, isTemplateEditable } = usePageTemplateContext();

    console.log('[PropertiesWithDividerWrapper] Rendering', {
        summaryElements,
        summaryElementsLength: summaryElements?.length,
        isTemplateEditable,
    });

    // Do not show properties section at all when there are no properties
    // and the current user has no permissions to manage properties.
    if (!summaryElements?.length && !isTemplateEditable) {
        console.log('[PropertiesWithDividerWrapper] Returning null - no properties to show');
        return null;
    }

    console.log('[PropertiesWithDividerWrapper] Rendering Properties component');
    return (
        <>
            <Properties />
            <StyledDivider />
        </>
    );
}
