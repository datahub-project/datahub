import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import DomainSearchResultItem from '@app/domainV2/DomainSearchResultItem';
import TestPageContainer, { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const adDeliveryDomain = {
    urn: 'urn:li:domain:ad-delivery',
    type: EntityType.Domain,
    id: 'ad-delivery',
    properties: { name: 'Ad Delivery' },
};

function renderItem(matchedOwnerName?: string) {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <DomainSearchResultItem
                    entity={adDeliveryDomain}
                    entityRegistry={getTestEntityRegistry()}
                    query="Sourabh"
                    matchedOwnerName={matchedOwnerName}
                    onResultClick={() => {}}
                />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('DomainSearchResultItem', () => {
    it('explains an owner match under the domain name', () => {
        renderItem('Sourabh Shrivastav');

        expect(screen.getByText('Ad Delivery')).toBeInTheDocument();
        expect(screen.getByTestId('domain-search-owner-match')).toHaveTextContent('Owned by Sourabh Shrivastav');
    });

    it('shows only the domain name for a name match', () => {
        renderItem(undefined);

        expect(screen.getByText('Ad Delivery')).toBeInTheDocument();
        expect(screen.queryByTestId('domain-search-owner-match')).not.toBeInTheDocument();
    });
});
