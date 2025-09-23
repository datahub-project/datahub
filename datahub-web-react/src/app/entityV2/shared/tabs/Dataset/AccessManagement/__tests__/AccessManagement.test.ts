import { handleAccessRoles } from '@app/entityV2/shared/tabs/Dataset/AccessManagement/utils';

import { GetExternalRolesQuery } from '@graphql/dataset.generated';

describe('handleAccessRoles (EntityV2)', () => {
    it('should properly map external roles using isAssignedToMe field', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: {
                    roles: [
                        {
                            role: {
                                id: 'test-role-granted',
                                properties: {
                                    name: 'Test Role Granted',
                                    description:
                                        'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                                    type: 'READ',
                                    requestUrl: 'https://www.google.com/role-granted',
                                },
                                urn: 'urn:li:role:test-role-granted',
                                isAssignedToMe: true,
                            },
                        },
                        {
                            role: {
                                id: 'test-role-not-granted',
                                properties: {
                                    name: 'Test Role Not Granted',
                                    description:
                                        'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                                    type: 'READ',
                                    requestUrl: 'https://www.google.com/role-not-granted',
                                },
                                urn: 'urn:li:role:test-role-not-granted',
                                isAssignedToMe: false,
                            },
                        },
                    ],
                },
                __typename: 'Dataset',
            },
        };

<<<<<<< HEAD
        const GetMeQueryUser: GetMeQuery = {
            me: {
                corpUser: {
                    urn: 'urn:li:corpuser:datahub',
                    username: 'datahub',
                    info: {
                        active: true,
                        displayName: 'DataHub',
                        title: 'DataHub Root User',
                        firstName: null,
                        lastName: null,
                        fullName: null,
                        email: null,
                        __typename: 'CorpUserInfo',
                    },
                    editableProperties: {
                        displayName: null,
                        title: null,
                        pictureLink:
                            'https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png',
                        teams: [],
                        skills: [],
                        __typename: 'CorpUserEditableProperties',
                    },
                    settings: {
                        appearance: {
                            showSimplifiedHomepage: false,
                            __typename: 'CorpUserAppearanceSettings',
                        },
                        views: null,
                        __typename: 'CorpUserSettings',
                    },
                    __typename: 'CorpUser',
                },
                platformPrivileges: {
                    viewAnalytics: true,
                    managePolicies: true,
                    viewMetadataProposals: true,
                    manageIdentities: true,
                    generatePersonalAccessTokens: true,
                    manageIngestion: true,
                    manageSecrets: true,
                    manageTokens: true,
                    manageDomains: true,
                    manageTests: true,
                    manageGlossaries: true,
                    manageUserCredentials: true,
                    manageTags: true,
                    viewTests: false,
                    viewManageTags: true,
                    createDomains: true,
                    createTags: true,
                    manageGlobalSettings: true,
                    manageGlobalViews: true,
                    manageOwnershipTypes: true,
                    manageGlobalAnnouncements: true,
                    manageDocumentationForms: true,
                    viewDocumentationFormsPage: true,
                    createBusinessAttributes: true,
                    manageBusinessAttributes: true,
                    manageStructuredProperties: true,
                    viewStructuredPropertiesPage: true,
                    manageApplications: true,
                    manageFeatures: true,
                    manageHomePageTemplates: true,
                    manageOrganizationDisplayPreferences: true,
                    proposeCreateGlossaryTerm: true,
                    proposeCreateGlossaryNode: true,
                    canViewIngestionPage: true,
                    createSupportTickets: true,
                    __typename: 'PlatformPrivileges',
                },
                __typename: 'AuthenticatedUser',
            },
        };
        const externalRole = handleAccessRoles(externalRolesQuery, GetMeQueryUser);
=======
        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toHaveLength(2);
>>>>>>> 57250477bfcbd08895244931a063a4bb3f7bbbf3
        expect(externalRole).toMatchObject([
            {
                name: 'Test Role Granted',
                description:
                    'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                accessType: 'READ',
                hasAccess: true, // Should be true since isAssignedToMe is true
                url: 'https://www.google.com/role-granted',
            },
            {
                name: 'Test Role Not Granted',
                description:
                    'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                accessType: 'READ',
                hasAccess: false, // Should be false since isAssignedToMe is false
                url: 'https://www.google.com/role-not-granted',
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

<<<<<<< HEAD
        const GetMeQueryUser: GetMeQuery = {
            me: {
                corpUser: {
                    urn: 'urn:li:corpuser:datahub',
                    username: 'datahub',
                    info: {
                        active: true,
                        displayName: 'DataHub',
                        title: 'DataHub Root User',
                        firstName: null,
                        lastName: null,
                        fullName: null,
                        email: null,
                        __typename: 'CorpUserInfo',
                    },
                    editableProperties: {
                        displayName: null,
                        title: null,
                        pictureLink:
                            'https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png',
                        teams: [],
                        skills: [],
                        __typename: 'CorpUserEditableProperties',
                    },
                    settings: {
                        appearance: {
                            showSimplifiedHomepage: false,
                            __typename: 'CorpUserAppearanceSettings',
                        },
                        views: null,
                        __typename: 'CorpUserSettings',
                    },
                    __typename: 'CorpUser',
                },
                platformPrivileges: {
                    viewAnalytics: true,
                    managePolicies: true,
                    viewMetadataProposals: true,
                    manageIdentities: true,
                    generatePersonalAccessTokens: true,
                    manageIngestion: true,
                    manageSecrets: true,
                    manageTokens: true,
                    manageDomains: true,
                    manageTests: true,
                    manageGlossaries: true,
                    manageUserCredentials: true,
                    manageTags: true,
                    viewTests: false,
                    viewManageTags: true,
                    createDomains: true,
                    createTags: true,
                    manageGlobalSettings: true,
                    manageGlobalViews: true,
                    manageOwnershipTypes: true,
                    manageGlobalAnnouncements: true,
                    manageDocumentationForms: true,
                    viewDocumentationFormsPage: true,
                    createBusinessAttributes: true,
                    manageBusinessAttributes: true,
                    manageStructuredProperties: true,
                    viewStructuredPropertiesPage: true,
                    manageApplications: true,
                    manageFeatures: true,
                    manageHomePageTemplates: true,
                    manageOrganizationDisplayPreferences: true,
                    proposeCreateGlossaryNode: true,
                    proposeCreateGlossaryTerm: true,
                    canViewIngestionPage: true,
                    createSupportTickets: true,
                    __typename: 'PlatformPrivileges',
                },
                __typename: 'AuthenticatedUser',
            },
        };
        const externalRole = handleAccessRoles(externalRolesQuery, GetMeQueryUser);
        expect(externalRole).toMatchObject([]);
=======
        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toHaveLength(0);
        expect(externalRole).toEqual([]);
>>>>>>> 57250477bfcbd08895244931a063a4bb3f7bbbf3
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

<<<<<<< HEAD
        const GetMeQueryUser: GetMeQuery = {
            me: {
                corpUser: {
                    urn: 'urn:li:corpuser:datahub',
                    username: 'datahub',
                    info: {
                        active: true,
                        displayName: 'DataHub',
                        title: 'DataHub Root User',
                        firstName: null,
                        lastName: null,
                        fullName: null,
                        email: null,
                        __typename: 'CorpUserInfo',
                    },
                    editableProperties: {
                        displayName: null,
                        title: null,
                        pictureLink:
                            'https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png',
                        teams: [],
                        skills: [],
                        __typename: 'CorpUserEditableProperties',
                    },
                    settings: {
                        appearance: {
                            showSimplifiedHomepage: false,
                            __typename: 'CorpUserAppearanceSettings',
                        },
                        views: null,
                        __typename: 'CorpUserSettings',
                    },
                    __typename: 'CorpUser',
                },
                platformPrivileges: {
                    viewAnalytics: true,
                    managePolicies: true,
                    viewMetadataProposals: true,
                    manageIdentities: true,
                    generatePersonalAccessTokens: true,
                    manageIngestion: true,
                    manageSecrets: true,
                    manageTokens: true,
                    manageDomains: true,
                    manageTests: true,
                    manageGlossaries: true,
                    manageUserCredentials: true,
                    manageTags: true,
                    viewTests: false,
                    viewManageTags: true,
                    createDomains: true,
                    createTags: true,
                    manageGlobalSettings: true,
                    manageGlobalViews: true,
                    manageOwnershipTypes: true,
                    manageGlobalAnnouncements: true,
                    manageDocumentationForms: true,
                    viewDocumentationFormsPage: true,
                    createBusinessAttributes: true,
                    manageBusinessAttributes: true,
                    manageStructuredProperties: true,
                    viewStructuredPropertiesPage: true,
                    manageApplications: true,
                    manageFeatures: true,
                    manageHomePageTemplates: true,
                    manageOrganizationDisplayPreferences: true,
                    proposeCreateGlossaryNode: true,
                    proposeCreateGlossaryTerm: true,
                    canViewIngestionPage: true,
                    createSupportTickets: true,
                    __typename: 'PlatformPrivileges',
                },
                __typename: 'AuthenticatedUser',
            },
        };
        const externalRole = handleAccessRoles(externalRolesQuery, GetMeQueryUser);
        expect(externalRole).toMatchObject([
            {
                name: 'accessRole',
                description:
                    'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                accessType: 'READ',
                hasAccess: false,
                url: 'https://www.google.com/',
            },
        ]);
=======
        const externalRole = handleAccessRoles(externalRolesQuery);
        expect(externalRole).toHaveLength(1);
        expect(externalRole[0]).toMatchObject({
            name: 'Minimal Role',
            description: ' ', // Should fallback to space
            accessType: ' ', // Should fallback to space
            hasAccess: false,
            url: undefined, // Should be undefined when requestUrl is null
        });
>>>>>>> 57250477bfcbd08895244931a063a4bb3f7bbbf3
    });
});
