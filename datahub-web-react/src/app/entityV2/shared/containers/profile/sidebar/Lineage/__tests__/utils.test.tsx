import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { getRelatedEntitySummary } from '@app/entityV2/shared/containers/profile/sidebar/Lineage/utils';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const entityRegistry = getTestEntityRegistry();

describe('Lineage getRelatedEntitySummary', () => {
    it('uses the singular entity name when count is 1', () => {
        const { container } = render(
            <>
                {getRelatedEntitySummary(
                    { total: 1, types: [{ type: EntityType.Dataset, count: 1, isEntityType: true }] },
                    entityRegistry,
                )}
            </>,
        );

        expect(container.textContent).toBe('1 dataset');
    });

    it('uses the plural collection name when count is greater than 1', () => {
        const { container } = render(
            <>
                {getRelatedEntitySummary(
                    { total: 5, types: [{ type: EntityType.Dataset, count: 5, isEntityType: true }] },
                    entityRegistry,
                )}
            </>,
        );

        expect(container.textContent).toBe('5 datasets');
    });

    it('renders sub-type labels verbatim, without singular/plural resolution', () => {
        const { container } = render(
            <>
                {getRelatedEntitySummary(
                    { total: 3, types: [{ type: 'Table', count: 3, isEntityType: false }] },
                    entityRegistry,
                )}
            </>,
        );

        expect(container.textContent).toBe('3 table');
    });
});
