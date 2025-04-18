import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import DomainAssociationRequestItem from '@app/actionrequestV2/item/DomainAssociationRequestItem';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { ActionRequest, ActionRequestOrigin, ActionRequestStatus, ActionRequestType } from '@types';

describe('DomainAsssociationRequestItem', () => {
    const defaultProps = {
        actionRequest: {
            urn: 'urn:li:actionRequest:123',
            type: ActionRequestType.DomainAssociation,
            status: ActionRequestStatus.Pending,
            origin: ActionRequestOrigin.Manual,
            created: {
                time: 1641034800000, // 2022-01-01
            },
            params: {
                domainProposal: {
                    domain: {
                        urn: 'urn:li:domain:testDomain',
                        properties: {
                            name: 'Testing',
                        },
                    },
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
                    <DomainAssociationRequestItem {...defaultProps} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(getByText('Testing')).toBeInTheDocument();
    });

    it('returns null when no domain is provided', () => {
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
                    <DomainAssociationRequestItem {...props} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container.firstChild).toBeNull();
    });
});
