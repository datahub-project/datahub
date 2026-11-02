import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import GlossaryRelatedTerms from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const { entityState, relatedTermsQuery } = vi.hoisted(() => ({
    entityState: {
        urn: '',
    },
    relatedTermsQuery: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({
        urn: entityState.urn,
    }),
}));

vi.mock('@graphql/glossary.generated', () => ({
    useGetRelatedTermsQuery: (...args: unknown[]) => relatedTermsQuery(...args),
}));

vi.mock('@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult', () => ({
    default: ({ glossaryRelatedTermResult }: { glossaryRelatedTermResult: Array<{ entity?: { urn?: string } }> }) => (
        <div data-testid="related-results">{glossaryRelatedTermResult.map((item) => item.entity?.urn).join(',')}</div>
    ),
}));

const currentTerm = {
    glossaryTerm: {
        urn: 'urn:li:glossaryTerm:current',
        hasRelatedTerms: {
            relationships: [{ entity: { urn: 'urn:li:glossaryTerm:child' } }],
        },
    },
};

describe('Glossary Related Terms', () => {
    beforeEach(() => {
        entityState.urn = '';
        relatedTermsQuery.mockReset();
        relatedTermsQuery.mockReturnValue({ data: undefined, loading: false, error: undefined });
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

    it('shows this term from getRelatedTerms and ignores a cached payload for another term', async () => {
        entityState.urn = 'urn:li:glossaryTerm:current';
        relatedTermsQuery.mockReturnValue({
            data: currentTerm,
            loading: false,
            error: undefined,
        });

        const { rerender } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => {
            expect(screen.getByTestId('related-results')).toHaveTextContent('urn:li:glossaryTerm:child');
        });
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

        entityState.urn = 'urn:li:glossaryTerm:next';
        relatedTermsQuery.mockReturnValue({
            data: currentTerm,
            loading: true,
            error: undefined,
        });
        rerender(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.queryByTestId('related-results')).not.toBeInTheDocument();

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
            error: undefined,
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

    it('asks for the next page of the selected group', async () => {
        entityState.urn = 'urn:li:glossaryTerm:current';
        const relationships = Array.from({ length: 10 }, (_, index) => ({
            entity: { urn: `urn:li:glossaryTerm:child-${index}` },
        }));
        relatedTermsQuery.mockReturnValue({
            data: {
                glossaryTerm: {
                    urn: 'urn:li:glossaryTerm:current',
                    hasRelatedTerms: { total: 11, relationships },
                },
            },
            loading: false,
            error: undefined,
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.click(await screen.findByTestId('load-more-related-terms'));

        expect(relatedTermsQuery).toHaveBeenLastCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    hasRelatedTermsCount: 20,
                    isRelatedTermsCount: 10,
                    isAChildrenCount: 10,
                    containedByCount: 10,
                }),
            }),
        );
    });

    it('shows an empty list when the query settles without a term', async () => {
        entityState.urn = 'urn:li:glossaryTerm:current';
        relatedTermsQuery.mockReturnValue({
            data: { glossaryTerm: null },
            loading: false,
            error: undefined,
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(screen.getByTestId('related-results')).toBeInTheDocument());
        expect(screen.getByTestId('related-results')).toHaveTextContent('');
    });

    it('shows an empty list when the query fails before any relationships arrive', async () => {
        entityState.urn = 'urn:li:glossaryTerm:current';
        relatedTermsQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: new Error('related terms failed'),
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(screen.getByTestId('related-results')).toBeInTheDocument());
        expect(screen.getByTestId('related-results')).toHaveTextContent('');
    });
});
