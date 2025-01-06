import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';
import {
    ActionRequest,
    ActionRequestOrigin,
    ActionRequestStatus,
    ActionRequestType,
    EntityType,
    StructuredPropertyDefinition,
} from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import StructuredPropertyAsssociationRequestItem from '../StructuredPropertyAsssociationRequestItem';

describe('StructuredPropertyAsssociationRequestItem', () => {
    const defaultProps = {
        actionRequest: {
            urn: 'urn:li:actionRequest:123',
            type: ActionRequestType.StructuredPropertyAssociation,
            status: ActionRequestStatus.Pending,
            origin: ActionRequestOrigin.Manual,
            created: {
                time: 1641034800000, // 2022-01-01
            },
            params: {
                structuredPropertyProposal: {
                    structuredProperties: [
                        {
                            structuredProperty: {
                                urn: 'urn:li:structuredProperty:123',
                                definition: {} as any as StructuredPropertyDefinition,
                                type: EntityType.StructuredProperty,
                            },
                            associatedUrn: 'urn:li:dataset:testDataset',
                            values: [
                                {
                                    __typename: 'StringValue',
                                    stringValue: 'Test Value',
                                },
                            ],
                        },
                    ],
                },
            },
            entity: {
                type: EntityType.Dataset,
                urn: 'urn:li:dataset:testDataset',
                name: 'Test Dataset',
            },
            description: 'Test description',
            assignedUsers: ['user1', 'user2'],
            assignedGroups: ['group1'],
            assignedRoles: ['role1'],
        } as ActionRequest,
        showActionsButtons: true,
        onUpdate: vi.fn(),
    };

    it('renders basic content correctly', () => {
        const { getByText } = render(
            <MockedProvider>
                <TestPageContainer>
                    <StructuredPropertyAsssociationRequestItem {...defaultProps} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(getByText('Test Value')).toBeInTheDocument();
        expect(getByText('Test Dataset')).toBeInTheDocument();
    });

    it('shows all values in popover on hover', async () => {
        const props = {
            ...defaultProps,
            actionRequest: {
                ...defaultProps.actionRequest,
                params: {
                    structuredPropertyProposal: {
                        structuredProperties: [
                            {
                                structuredProperty: {
                                    urn: 'urn:li:structuredProperty:123',
                                    definition: {} as any as StructuredPropertyDefinition,
                                    type: EntityType.StructuredProperty,
                                },
                                associatedUrn: 'urn:li:dataset:testDataset',
                                values: [
                                    {
                                        __typename: 'StringValue' as const,
                                        stringValue: 'Value 1',
                                    },
                                    {
                                        __typename: 'StringValue' as const,
                                        stringValue: 'Value 2',
                                    },
                                ],
                            },
                        ],
                    },
                },
            },
        };

        const { getByText, findByText } = render(
            <MockedProvider>
                <TestPageContainer>
                    <StructuredPropertyAsssociationRequestItem {...props} />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.mouseEnter(getByText('& 1 others'));

        expect(await findByText('Value 1')).toBeInTheDocument();
        expect(await findByText('Value 2')).toBeInTheDocument();
    });

    it('returns null when no structured property is provided', () => {
        const props = {
            ...defaultProps,
            actionRequest: {
                ...defaultProps.actionRequest,
                params: {
                    structuredPropertyProposal: {
                        structuredProperties: [],
                    },
                },
            },
        };

        const { container } = render(
            <MockedProvider>
                <TestPageContainer>
                    <StructuredPropertyAsssociationRequestItem {...props} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container.firstChild).toBeNull();
    });
});
