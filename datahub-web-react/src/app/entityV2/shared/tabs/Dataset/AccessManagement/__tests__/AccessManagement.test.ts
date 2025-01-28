import { GetExternalRolesQuery } from '../../../../../../../graphql/dataset.generated';
import { GetMeQuery } from '../../../../../../../graphql/me.generated';
import { handleAccessRoles } from '../utils';

describe('handleAccessRoles', () => {
    it('should properly map the externalroles and loggedin user', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: {
                    roles: [
                        {
                            role: {
                                id: 'accessRole',
                                properties: {
                                    name: 'accessRole',
                                    description:
                                        'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                                    type: 'READ',
                                    requestUrl: 'https://www.google.com/',
                                },
                                urn: 'urn:li:role:accessRole',
                                isAssignedToMe: true,
                            },
                        },
                    ],
                },
                __typename: 'Dataset',
            },
        };

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
                    manageIdentities: true,
                    manageDomains: true,
                    manageTags: true,
                    createDomains: true,
                    createTags: true,
                    manageUserCredentials: true,
                    manageGlossaries: true,
                    viewTests: false,
                    manageTests: true,
                    manageTokens: true,
                    manageSecrets: true,
                    manageIngestion: true,
                    generatePersonalAccessTokens: true,
                    manageGlobalViews: true,
                    manageOwnershipTypes: true,
                    manageGlobalAnnouncements: true,
                    createBusinessAttributes: true,
                    manageBusinessAttributes: true,
                    manageStructuredProperties: true,
                    viewStructuredPropertiesPage: true,

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
    });
    it('should return empty array', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: null,
                __typename: 'Dataset',
            },
        };

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
                    manageIdentities: true,
                    manageDomains: true,
                    manageTags: true,
                    createDomains: true,
                    createTags: true,
                    manageUserCredentials: true,
                    manageGlossaries: true,
                    viewTests: false,
                    manageTests: true,
                    manageTokens: true,
                    manageSecrets: true,
                    manageIngestion: true,
                    generatePersonalAccessTokens: true,
                    manageGlobalViews: true,
                    manageOwnershipTypes: true,
                    manageGlobalAnnouncements: true,
                    createBusinessAttributes: true,
                    manageBusinessAttributes: true,
                    manageStructuredProperties: true,
                    viewStructuredPropertiesPage: true,
                    __typename: 'PlatformPrivileges',
                },
                __typename: 'AuthenticatedUser',
            },
        };
        const externalRole = handleAccessRoles(externalRolesQuery, GetMeQueryUser);
        expect(externalRole).toMatchObject([]);
    });
    it('should properly map the externalroles and loggedin user and access true', () => {
        const externalRolesQuery: GetExternalRolesQuery = {
            dataset: {
                access: {
                    roles: [
                        {
                            role: {
                                id: 'accessRole',
                                properties: {
                                    name: 'accessRole',
                                    description:
                                        'This role access is required by the developers to test and deploy the code also adding few more details to check the description length for the given data and hence check the condition of read more and read less ',
                                    type: 'READ',
                                    requestUrl: 'https://www.google.com/',
                                },
                                urn: 'urn:li:role:accessRole',
                                isAssignedToMe: true,
                            },
                        },
                    ],
                },
                __typename: 'Dataset',
            },
        };

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
                    manageIdentities: true,
                    manageDomains: true,
                    manageTags: true,
                    createDomains: true,
                    createTags: true,
                    manageUserCredentials: true,
                    manageGlossaries: true,
                    viewTests: false,
                    manageTests: true,
                    manageTokens: true,
                    manageSecrets: true,
                    manageIngestion: true,
                    generatePersonalAccessTokens: true,
                    manageGlobalViews: true,
                    manageOwnershipTypes: true,
                    manageGlobalAnnouncements: true,
                    createBusinessAttributes: true,
                    manageBusinessAttributes: true,
                    manageStructuredProperties: true,
                    viewStructuredPropertiesPage: true,

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
    });
});
