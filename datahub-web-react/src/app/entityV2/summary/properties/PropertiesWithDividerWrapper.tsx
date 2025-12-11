/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import Properties from '@app/entityV2/summary/properties/Properties';
import { StyledDivider } from '@app/entityV2/summary/styledComponents';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

/**
 * The wrapper to show/hide divider with the whole section
 */
export default function PropertiesWithDividerWrapper() {
    const { summaryElements, isTemplateEditable } = usePageTemplateContext();

    // Do not show properties section at all when there are no properties
    // and the current user has no permissions to manage properties.
    if (!summaryElements?.length && !isTemplateEditable) return null;

    return (
        <>
            <Properties />
            <StyledDivider />
        </>
    );
}
