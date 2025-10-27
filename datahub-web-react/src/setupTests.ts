// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom/vitest';

// Mock window.matchMedia interface.
// See https://jestjs.io/docs/en/manual-mocks#mocking-methods-which-are-not-implemented-in-jsdom
// and https://github.com/ant-design/ant-design/issues/21096.
global.matchMedia =
    global.matchMedia ||
    (() => {
        return {
            matches: false,
            addListener: vi.fn(),
            removeListener: vi.fn(),
        };
    });

window.location = {
    ...window.location,
    replace: () => {},
};

// Suppress `Error: Not implemented: window.computedStyle(elt, pseudoElt)`.
// From https://github.com/vitest-dev/vitest/issues/2061
// and https://github.com/NickColley/jest-axe/issues/147#issuecomment-758804533
const { getComputedStyle } = window;
window.getComputedStyle = (elt) => getComputedStyle(elt);

vi.mock('js-cookie', () => ({
    default: {
        get: () => 'urn:li:corpuser:2',
    },
}));
vi.mock('./app/entity/shared/tabs/Documentation/components/editor/Editor');

vi.stubGlobal(
    'ResizeObserver',
    vi.fn(() => ({
        observe: vi.fn(),
        unobserve: vi.fn(),
        disconnect: vi.fn(),
    })),
);

vi.stubGlobal(
    'IntersectionObserver',
    vi.fn(() => ({
        observe: vi.fn(),
        unobserve: vi.fn(),
        disconnect: vi.fn(),
        root: null,
        rootMargin: '',
        thresholds: [],
    })),
);

// Global mock for GraphQL settings to provide missing fragments
vi.mock('@graphql/settings.generated', () => ({
    // Fragment exports that other generated files need
    AssetSettingsFieldsFragment: {},
    AssetSettingsFieldsFragmentDoc: `fragment AssetSettingsFields on AssetSettings {
        assetSummary {
            templates {
                template {
                    urn
                    type
                }
            }
        }
    }`,
    NotificationSettingsFieldsFragment: {},
    NotificationSettingsFieldsFragmentDoc: `fragment notificationSettingsFields on NotificationSettings {
        sinkTypes
        slackSettings {
            channels
        }
        emailSettings {
            email
        }
        teamsSettings {
            channels {
                id
                name
            }
        }
        settings {
            type
            value
            params {
                key
                value
            }
        }
    }`,

    // Mock hooks with default implementations
    useGetGlobalSettingsQuery: vi.fn(() => ({ data: undefined, loading: false, error: undefined })),
    useGetUserNotificationSettingsQuery: vi.fn(() => ({ data: undefined, loading: false, error: undefined })),
    useGetGroupNotificationSettingsQuery: vi.fn(() => ({ data: undefined, loading: false, error: undefined })),
    useGetSsoSettingsQuery: vi.fn(() => ({ data: undefined, loading: false, error: undefined })),
    useUpdateUserNotificationSettingsMutation: vi.fn(() => [
        vi.fn().mockResolvedValue({ data: {} }),
        { data: undefined, loading: false, error: undefined },
    ]),
    useUpdateGroupNotificationSettingsMutation: vi.fn(() => [
        vi.fn(),
        { data: undefined, loading: false, error: undefined },
    ]),
    useUpdateGlobalIntegrationSettingsMutation: vi.fn(() => [
        vi.fn(),
        { data: undefined, loading: false, error: undefined },
    ]),
    useUpdateGlobalNotificationSettingsMutation: vi.fn(() => [
        vi.fn(),
        { data: undefined, loading: false, error: undefined },
    ]),
    useUpdateSsoSettingsMutation: vi.fn(() => [vi.fn(), { data: undefined, loading: false, error: undefined }]),
    useUpdateAssetSettingsMutation: vi.fn(() => [vi.fn(), { data: undefined, loading: false, error: undefined }]),
    useUpdateHelpLinkMutation: vi.fn(() => [vi.fn(), { data: undefined, loading: false, error: undefined }]),
    useUpdateOrganizationDisplayPreferencesMutation: vi.fn(() => [
        vi.fn(),
        { data: undefined, loading: false, error: undefined },
    ]),

    // Document exports - provide valid GraphQL document structures
    UpdateUserNotificationSettingsDocument: {
        kind: 'Document',
        definitions: [
            {
                kind: 'OperationDefinition',
                operation: 'mutation',
                name: { kind: 'Name', value: 'updateUserNotificationSettings' },
            },
        ],
    },
    GetGlobalSettingsDocument: {
        kind: 'Document',
        definitions: [
            {
                kind: 'OperationDefinition',
                operation: 'query',
                name: { kind: 'Name', value: 'getGlobalSettings' },
            },
        ],
    },
}));
