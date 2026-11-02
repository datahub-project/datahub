import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useGlossaryRelatedTermsTabCount from '@app/entityV2/glossaryTerm/profile/useGlossaryRelatedTermsTabCount';

const { entityState, relatedTermsQuery } = vi.hoisted(() => ({
    entityState: { urn: '' },
    relatedTermsQuery: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: entityState.urn }),
}));

vi.mock('@graphql/glossary.generated', () => ({
    useGetRelatedTermsQuery: (...args: unknown[]) => relatedTermsQuery(...args),
}));

function CountProbe() {
    const count = useGlossaryRelatedTermsTabCount();
    return <div data-testid="count">{count === undefined ? 'none' : String(count)}</div>;
}

describe('useGlossaryRelatedTermsTabCount', () => {
    beforeEach(() => {
        entityState.urn = 'urn:li:glossaryTerm:current';
        relatedTermsQuery.mockReset();
    });

    it('sums totals from getRelatedTerms for the urn on screen', () => {
        relatedTermsQuery.mockReturnValue({
            data: {
                glossaryTerm: {
                    urn: 'urn:li:glossaryTerm:current',
                    hasRelatedTerms: { total: 12 },
                    isAChildren: { total: 3 },
                },
            },
        });

        render(<CountProbe />);

        expect(screen.getByTestId('count')).toHaveTextContent('15');
        expect(relatedTermsQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    urn: 'urn:li:glossaryTerm:current',
                    isRelatedTermsCount: 10,
                    hasRelatedTermsCount: 10,
                    isAChildrenCount: 10,
                    containedByCount: 10,
                },
                fetchPolicy: 'cache-and-network',
            }),
        );
    });

    it('hides the count while the payload still belongs to another term', () => {
        relatedTermsQuery.mockReturnValue({
            data: {
                glossaryTerm: {
                    urn: 'urn:li:glossaryTerm:previous',
                    hasRelatedTerms: { total: 4 },
                },
            },
        });

        render(<CountProbe />);

        expect(screen.getByTestId('count')).toHaveTextContent('none');
    });
});
