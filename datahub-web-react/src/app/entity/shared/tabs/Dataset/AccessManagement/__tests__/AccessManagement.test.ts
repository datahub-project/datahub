import { handleAccessRoles } from '@app/entity/shared/tabs/Dataset/AccessManagement/utils';

import { GetExternalRolesQuery } from '@graphql/dataset.generated';

describe('handleAccessRoles', () => {
    it('should properly map the externalroles', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: {
                    roles: [
                        {
                            role: {
                                id: 'test-role-1',
                                properties: {
                                    name: 'Test Role 1',
                                    description:
                                        'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                                    type: 'READ',
                                    requestUrl: 'https://www.google.com/role-1',
                                },
                                urn: 'urn:li:role:test-role-1',
                                isAssignedToMe: true,
                            },
                        },
                        {
                            role: {
                                id: 'test-role-2',
                                properties: {
                                    name: 'Test Role 2',
                                    description:
                                        'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                                    type: 'READ',
                                    requestUrl: 'https://www.google.com/role-2',
                                },
                                urn: 'urn:li:role:test-role-2',
                                isAssignedToMe: false,
                            },
                        },
                    ],
                },
                __typename: 'Dataset',
            },
        };

        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toMatchObject([
            {
                name: 'Test Role 1',
                description:
                    'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                accessType: 'READ',
                hasAccess: true,
                url: 'https://www.google.com/role-1',
            },
            {
                name: 'Test Role 2',
                description:
                    'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                accessType: 'READ',
                hasAccess: false,
                url: 'https://www.google.com/role-2',
            },
        ]);
    });
    it('should return empty array when no access data is provided', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: null,
                __typename: 'Dataset',
            },
        };

        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toHaveLength(0);
        expect(externalRole).toEqual([]);
    });

    it('should handle roles without isAssignedToMe field gracefully', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: {
                    roles: [
                        {
                            role: {
                                id: 'test-role-without-assignment',
                                properties: {
                                    name: 'Test Role Without Assignment',
                                    description: 'A role without isAssignedToMe field',
                                    type: 'READ',
                                    requestUrl: 'https://www.example.com/role',
                                },
                                urn: 'urn:li:role:test-role-without-assignment',
                                // Note: isAssignedToMe is intentionally missing
                            } as any,
                        },
                    ],
                },
                __typename: 'Dataset',
            },
        };

        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toHaveLength(1);
        expect(externalRole[0]).toMatchObject({
            name: 'Test Role Without Assignment',
            description: 'A role without isAssignedToMe field',
            accessType: 'READ',
            hasAccess: undefined, // Should be undefined when isAssignedToMe is not present
            url: 'https://www.example.com/role',
        });
    });

    it('should handle roles with missing properties gracefully', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: {
                    roles: [
                        {
                            role: {
                                id: 'minimal-role',
                                properties: {
                                    name: 'Minimal Role',
                                    description: null,
                                    type: null,
                                    requestUrl: null,
                                } as any,
                                urn: 'urn:li:role:minimal-role',
                                isAssignedToMe: false,
                            },
                        },
                    ],
                },
                __typename: 'Dataset',
            },
        };

        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toHaveLength(1);
        expect(externalRole[0]).toMatchObject({
            name: 'Minimal Role',
            description: ' ', // Should fallback to space
            accessType: ' ', // Should fallback to space
            hasAccess: false,
            url: undefined, // Should be undefined when requestUrl is null
        });
    });
});
