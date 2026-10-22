import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import GlossaryRelatedTerms from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const { entityState, relatedTermsQuery } = vi.hoisted(() => ({
    entityState: {
        urn: '',
        entityData: null as {
            urn?: string;
            hasRelatedTerms?: { relationships: Array<{ entity: { urn: string } }> };
        } | null,
        loading: false,
        refetch: vi.fn(),
    },
    relatedTermsQuery: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({
        urn: entityState.urn,
        entityData: entityState.entityData,
        loading: entityState.loading,
    }),
    useRefetch: () => entityState.refetch,
}));

vi.mock('@graphql/glossary.generated', () => ({
    useGetRelatedTermsQuery: (...args: unknown[]) => relatedTermsQuery(...args),
}));

vi.mock('@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult', () => ({
    default: ({ glossaryRelatedTermResult }: { glossaryRelatedTermResult: Array<{ entity?: { urn?: string } }> }) => (
        <div data-testid="related-results">{glossaryRelatedTermResult.map((item) => item.entity?.urn).join(',')}</div>
    ),
}));

describe('Glossary Related Terms', () => {
    beforeEach(() => {
        entityState.urn = '';
        entityState.entityData = null;
        entityState.loading = false;
        entityState.refetch.mockClear();
        relatedTermsQuery.mockReset();
        relatedTermsQuery.mockReturnValue({ data: undefined, loading: false });
    });

    it('renders and print hasRelatedTerms detail by default', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Contains')).toBeInTheDocument();
        expect(getByText('Inherits')).toBeInTheDocument();
    });

    it('shows the next term relationships when the tab stays mounted across navigation', async () => {
        entityState.urn = 'urn:li:glossaryTerm:current';
        entityState.entityData = {
            urn: 'urn:li:glossaryTerm:current',
            hasRelatedTerms: { relationships: [{ entity: { urn: 'urn:li:glossaryTerm:next' } }] },
        };
        relatedTermsQuery.mockReturnValue({
            data: {
                glossaryTerm: {
                    urn: 'urn:li:glossaryTerm:current',
                    hasRelatedTerms: { relationships: [{ entity: { urn: 'urn:li:glossaryTerm:next' } }] },
                },
            },
            loading: false,
        });

        const { rerender } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => {
            expect(screen.getByTestId('related-results')).toHaveTextContent('urn:li:glossaryTerm:next');
        });
        expect(relatedTermsQuery).toHaveBeenCalledWith(
            expect.objectContaining({ variables: { urn: 'urn:li:glossaryTerm:current', count: 100 } }),
        );

        entityState.urn = 'urn:li:glossaryTerm:next';
        entityState.entityData = { urn: 'urn:li:glossaryTerm:current' };
        entityState.loading = false;
        relatedTermsQuery.mockReturnValue({ data: undefined, loading: true });

        rerender(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.queryByTestId('related-results')).not.toBeInTheDocument();
        await waitFor(() => expect(entityState.refetch).toHaveBeenCalled());

        entityState.entityData = {
            urn: 'urn:li:glossaryTerm:next',
            hasRelatedTerms: { relationships: [] },
        };
        relatedTermsQuery.mockReturnValue({
            data: {
                glossaryTerm: {
                    urn: 'urn:li:glossaryTerm:next',
                    hasRelatedTerms: {
                        relationships: [
                            { entity: { urn: 'urn:li:glossaryTerm:child-a' } },
                            { entity: { urn: 'urn:li:glossaryTerm:child-b' } },
                        ],
                    },
                },
            },
            loading: false,
        });

        rerender(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByTestId('related-results')).toHaveTextContent(
            'urn:li:glossaryTerm:child-a,urn:li:glossaryTerm:child-b',
        );
    });
});
