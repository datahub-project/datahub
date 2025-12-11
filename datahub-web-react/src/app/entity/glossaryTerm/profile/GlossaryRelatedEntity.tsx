/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EmbeddedListSearchSection } from '@app/entity/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

export default function GlossaryRelatedEntity() {
    const { entityData } = useEntityData();

    const entityUrn = entityData?.urn;

    const fixedOrFilters =
        (entityUrn && [
            {
                field: 'glossaryTerms',
                values: [entityUrn],
            },
            {
                field: 'fieldGlossaryTerms',
                values: [entityUrn],
            },
        ]) ||
        [];

    entityData?.isAChildren?.relationships?.forEach((term) => {
        const childUrn = term.entity?.urn;

        if (childUrn) {
            fixedOrFilters.push({
                field: 'glossaryTerms',
                values: [childUrn],
            });

            fixedOrFilters.push({
                field: 'fieldGlossaryTerms',
                values: [childUrn],
            });
        }
    });

    return (
        <EmbeddedListSearchSection
            fixedFilters={{
                unionType: UnionType.OR,
                filters: fixedOrFilters,
            }}
            emptySearchQuery="*"
            placeholderText="Filter entities..."
            skipCache
            applyView
        />
    );
}
