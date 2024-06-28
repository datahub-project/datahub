import { handleAccessRoles } from '../utils';
import { GetExternalRolesQuery } from '../../../../../../../graphql/dataset.generated';

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
    it('should return empty array', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: null,
                __typename: 'Dataset',
            },
        };

        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toMatchObject([]);
    });
});
