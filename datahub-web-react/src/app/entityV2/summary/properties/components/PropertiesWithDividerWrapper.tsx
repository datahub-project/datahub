import React from 'react';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';

import { StyledDivider } from '../../styledComponents';
import Properties from './Properties';

/**
 * The wrapper to show/hide divider with the whole section
 */
export default function PropertiesWithDividerWrapper() {
    const { properties, editable } = useAssetPropertiesContext();

    // Do not show properties section at all when there are no properties
    // and the current user has no permissions to manage properties
    if (!properties?.length && !editable) return null;

    return (
        <>
            <Properties />
            <StyledDivider />
        </>
    );
}
