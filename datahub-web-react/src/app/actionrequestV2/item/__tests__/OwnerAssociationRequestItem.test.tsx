import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import {
    ActionRequest,
    ActionRequestOrigin,
    ActionRequestStatus,
    ActionRequestType,
} from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import OwnerAssociationRequestItem from '../OwnerAssociationRequestItem';

describe('OwnerAsssociationRequestItem', () => {
    const defaultProps = {
        actionRequest: {
            urn: 'urn:li:actionRequest:123',
            type: ActionRequestType.OwnerAssociation,
            status: ActionRequestStatus.Pending,
            origin: ActionRequestOrigin.Manual,
            created: {
                time: 1641034800000, // 2022-01-01
            },
            params: {
                ownerProposal: {
                    owners: [
                        {
                            owner: {
                                urn: 'urn:li:corpuser:testUser',
                                type: 'CORP_USER',
                                username: 'Testing',
                                __typename: 'CorpUser',
                            },
                            associatedUrn: 'urn:li:corpuser:testUser',
                        },
                    ],
                },
            },
        } as ActionRequest,
        showActionsButtons: true,
        onUpdate: vi.fn(),
    };

    it('renders basic content correctly', () => {
        const { getByText } = render(
            <MockedProvider>
                <TestPageContainer>
                    <OwnerAssociationRequestItem {...defaultProps} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(getByText('Testing')).toBeInTheDocument();
    });

    it('returns null when no owner is provided', () => {
        const props = {
            ...defaultProps,
            actionRequest: {
                ...defaultProps.actionRequest,
                params: {},
            },
        };

        const { container } = render(
            <MockedProvider>
                <TestPageContainer>
                    <OwnerAssociationRequestItem {...props} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container.firstChild).toBeNull();
    });
});
