import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// AutoCompleteEntityItem is only used on the propagated path and pulls in a large
// subtree; stub it so these tests stay focused on the attribution branching.
vi.mock('@app/searchV2/autoCompleteV2/AutoCompleteEntityItem', () => ({
    default: () => <div data-testid="origin-entity" />,
}));

const renderCard = (propagationDetails: any) =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <div data-testid="wrapper">
                    <HoverCardAttributionDetails propagationDetails={propagationDetails} />
                </div>
            </TestPageContainer>
        </MockedProvider>,
    );

describe('HoverCardAttributionDetails', () => {
    it('shows the ingestion source for an externally-ingested association', () => {
        renderCard({
            attribution: {
                time: 1700000000000,
                sourceDetail: [
                    { key: 'external', value: 'true' },
                    { key: 'origin', value: 'lake-formation' },
                ],
            },
        });
        // Origin marker is rendered as a human-readable label, not an entity link.
        expect(screen.getByText('Lake Formation')).toBeInTheDocument();
        expect(screen.queryByTestId('origin-entity')).not.toBeInTheDocument();
    });

    it('renders nothing when the association is neither propagated nor external', () => {
        renderCard({ attribution: { time: 1700000000000, sourceDetail: [] } });
        expect(screen.getByTestId('wrapper')).toBeEmptyDOMElement();
    });
});
