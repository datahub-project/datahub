import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { getContentsSummaryText } from '@app/entityV2/shared/containers/profile/sidebar/Domain/utils';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const entityRegistry = getTestEntityRegistry();

function renderSummary(summary: Parameters<typeof getContentsSummaryText>[0]) {
    return render(<CustomThemeProvider>{getContentsSummaryText(summary, entityRegistry)}</CustomThemeProvider>);
}

describe('Domain getContentsSummaryText', () => {
    it('uses the singular entity name when count is 1', () => {
        const { container } = renderSummary({ total: 1, types: [{ entityType: EntityType.Dataset, count: 1 }] });

        expect(container.textContent).toBe('1 dataset');
    });

    it('uses the plural collection name when count is greater than 1', () => {
        const { container } = renderSummary({ total: 5, types: [{ entityType: EntityType.Dataset, count: 5 }] });

        expect(container.textContent).toBe('5 datasets');
    });

    it('renders sub-type labels verbatim, without singular/plural resolution', () => {
        const { container } = renderSummary({
            total: 3,
            types: [{ entityType: EntityType.Dataset, type: 'Table', count: 3 }],
        });

        expect(container.textContent).toBe('3 table');
    });
});
