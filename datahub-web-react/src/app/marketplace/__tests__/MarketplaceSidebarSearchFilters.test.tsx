import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import MarketplaceSidebarSearchFilters from '@app/marketplace/MarketplaceSidebarSearchFilters';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

describe('MarketplaceSidebarSearchFilters', () => {
    it('hides all filters when no options and no selections exist', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <MarketplaceSidebarSearchFilters
                        selectedDomainUrns={[]}
                        selectedOwnerUrns={[]}
                        selectedTagUrns={[]}
                        domainOptions={[]}
                        ownerOptions={[]}
                        tagOptions={[]}
                        onDomainsChange={vi.fn()}
                        onOwnersChange={vi.fn()}
                        onTagsChange={vi.fn()}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.queryByTestId('marketplace-sidebar-domain-filter')).not.toBeInTheDocument();
        expect(screen.queryByTestId('marketplace-sidebar-owners-filter')).not.toBeInTheDocument();
        expect(screen.queryByTestId('marketplace-sidebar-tag-filter')).not.toBeInTheDocument();
    });

    it('renders only filters that have options or active selections', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <MarketplaceSidebarSearchFilters
                        selectedDomainUrns={['urn:li:domain:engineering']}
                        selectedOwnerUrns={[]}
                        selectedTagUrns={[]}
                        domainOptions={[]}
                        ownerOptions={[
                            {
                                value: 'urn:li:corpuser:alice',
                                label: 'Alice',
                                creator: {
                                    urn: 'urn:li:corpuser:alice',
                                    type: EntityType.CorpUser,
                                    displayName: 'Alice',
                                },
                            },
                        ]}
                        tagOptions={[]}
                        onDomainsChange={vi.fn()}
                        onOwnersChange={vi.fn()}
                        onTagsChange={vi.fn()}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('marketplace-sidebar-domain-filter')).toBeInTheDocument();
        expect(screen.getByTestId('marketplace-sidebar-owners-filter')).toBeInTheDocument();
        expect(screen.queryByTestId('marketplace-sidebar-tag-filter')).not.toBeInTheDocument();
    });
});
