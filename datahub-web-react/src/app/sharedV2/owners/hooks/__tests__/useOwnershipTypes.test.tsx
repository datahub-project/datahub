import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import useOwnershipTypes from '@app/sharedV2/owners/hooks/useOwnershipTypes';

import { ListOwnershipTypesDocument, ListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { EntityType } from '@types';

describe('useOwnershipTypes Hook', () => {
    it('should return ownershipTypes and defaultOwnershipType when data is present', async () => {
        const wrapper = ({ children }) => {
            // FYI: __typename is required here as ListOwnershipTypesResult uses fragments
            const data: ListOwnershipTypesQuery = {
                __typename: 'Query',
                listOwnershipTypes: {
                    __typename: 'ListOwnershipTypesResult',
                    total: 2,
                    start: 0,
                    count: 10,
                    ownershipTypes: [
                        {
                            __typename: 'OwnershipTypeEntity',
                            urn: 'urn:li:ownershipType:BusinessOwner',
                            type: EntityType.CustomOwnershipType,
                            status: null,
                            info: {
                                __typename: 'OwnershipTypeInfo',
                                name: 'Business Owner',
                                description: 'Description',
                                created: null,
                                lastModified: null,
                            },
                        },
                        {
                            __typename: 'OwnershipTypeEntity',
                            urn: 'urn:li:ownershipType:TechnicalOwner',
                            type: EntityType.CustomOwnershipType,
                            status: null,
                            info: {
                                __typename: 'OwnershipTypeInfo',
                                name: 'Technical Owner',
                                description: 'Description',
                                created: null,
                                lastModified: null,
                            },
                        },
                    ],
                },
            };
            return (
                <MockedProvider
                    mocks={[
                        {
                            request: {
                                query: ListOwnershipTypesDocument,
                                variables: { input: {} },
                            },
                            result: {
                                data,
                            },
                        },
                    ]}
                    addTypename={false}
                >
                    {children}
                </MockedProvider>
            );
        };
        const { result, waitForNextUpdate } = renderHook(() => useOwnershipTypes(), { wrapper });
        await waitForNextUpdate();

        expect(result.current.ownershipTypes.length).toEqual(2);
        expect(result.current.ownershipTypes[0].urn).toEqual('urn:li:ownershipType:BusinessOwner');
        expect(result.current.ownershipTypes[1].urn).toEqual('urn:li:ownershipType:TechnicalOwner');
        expect(result.current.defaultOwnershipType).toBe('urn:li:ownershipType:BusinessOwner');
    });

    it('should return empty array and undefined if data is missing', async () => {
        const wrapper = ({ children }) => <MockedProvider>{children}</MockedProvider>;
        const { result, waitForNextUpdate } = renderHook(() => useOwnershipTypes(), { wrapper });

        await waitForNextUpdate();

        expect(result.current.ownershipTypes).toEqual([]);
        expect(result.current.defaultOwnershipType).toBeUndefined();
    });
});
