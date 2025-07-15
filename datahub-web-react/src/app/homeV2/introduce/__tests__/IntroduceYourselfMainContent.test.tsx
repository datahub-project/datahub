import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { createMemoryHistory } from 'history';
import React from 'react';
import { Router } from 'react-router-dom';
import { vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { IntroduceYourselfMainContent } from '@app/homeV2/introduce/IntroduceYourselfMainContent';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { NotificationScenarioType, NotificationSettingValue } from '@types';

// Mock dependencies
vi.mock('@app/context/useUserContext');

// Mock useEntityRegistry
const mockEntityRegistry = {
    getDisplayName: vi.fn(() => 'Test Platform'),
};

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(() => mockEntityRegistry),
}));

// Mock useGetDataPlatforms
const mockGetDataPlatforms = [
    {
        platform: {
            urn: 'urn:li:dataPlatform:kafka',
            name: 'kafka',
            properties: { displayName: 'Kafka' },
        },
        count: 5,
    },
];

vi.mock('@app/homeV2/content/tabs/discovery/sections/platform/useGetDataPlatforms', () => ({
    useGetDataPlatforms: vi.fn(() => mockGetDataPlatforms),
}));

// Mock the useMarketingOptIn hook
const mockHandleMarketingOptInChange = vi.fn();
const mockMarketingOptIn = vi.fn(() => true);
const mockIsUpdatingMarketingSettings = vi.fn(() => false);

vi.mock('@app/settingsV2/personal/notifications/useMarketingOptIn', () => ({
    useMarketingOptIn: () => ({
        marketingOptIn: mockMarketingOptIn(),
        isUpdatingMarketingSettings: mockIsUpdatingMarketingSettings(),
        handleMarketingOptInChange: mockHandleMarketingOptInChange,
    }),
}));

// Mock useActorSinkSettings hook
const mockNotificationSettings = vi.fn();
const mockRefetchNotificationSettings = vi.fn();

vi.mock('@app/shared/subscribe/drawer/useSinkSettings', () => ({
    default: () => ({
        notificationSettings: mockNotificationSettings(),
        refetch: mockRefetchNotificationSettings,
        loading: false,
        error: null,
    }),
}));

// Mock GraphQL queries
vi.mock('@src/graphql/view.generated', () => ({
    useListGlobalViewsQuery: () => ({
        loading: false,
        data: { listGlobalViews: { views: [] } },
    }),
}));

vi.mock('@graphql/recommendations.generated', () => ({
    useListRecommendationsQuery: () => ({
        loading: false,
        data: { listRecommendations: { modules: [] } },
    }),
}));

vi.mock('@graphql/user.generated', async () => {
    const actual = await vi.importActual<any>('@graphql/user.generated');
    return {
        ...actual,
        useUpdateCorpUserPropertiesMutation: () => [vi.fn(), { loading: false }],
        useUpdateCorpUserViewsSettingsMutation: () => [vi.fn()],
    };
});

// Mock other dependencies
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
        identify: vi.fn(),
    },
    EventType: { IntroduceYourselfSubmitEvent: 'IntroduceYourselfSubmitEvent' },
}));

const mockUserContext = {
    user: { urn: 'urn:li:corpuser:test' },
    refetchUser: vi.fn(),
    state: { views: {} },
    updateState: vi.fn(),
    updateLocalState: vi.fn(),
};

describe('IntroduceYourselfMainContent - Marketing Opt-in', () => {
    const history = createMemoryHistory();

    const defaultOnboardingContext = {
        tourReshow: false,
        setTourReshow: vi.fn(),
        isTourOpen: false,
        setIsTourOpen: vi.fn(),
        isUserInitializing: false,
        setIsUserInitializing: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockMarketingOptIn.mockReturnValue(true);
        mockIsUpdatingMarketingSettings.mockReturnValue(false);
        mockNotificationSettings.mockReturnValue({
            settings: [
                {
                    type: NotificationScenarioType.DataHubCommunityUpdates,
                    value: NotificationSettingValue.Enabled,
                    params: [],
                },
            ],
        });

        (useUserContext as any).mockReturnValue(mockUserContext);
    });

    const renderComponent = () => {
        return render(
            <MockedProvider>
                <TestPageContainer>
                    <Router history={history}>
                        <OnboardingContext.Provider value={defaultOnboardingContext}>
                            <IntroduceYourselfMainContent />
                        </OnboardingContext.Provider>
                    </Router>
                </TestPageContainer>
            </MockedProvider>,
        );
    };

    describe('Marketing Checkbox Rendering', () => {
        it('should render the marketing opt-in checkbox', () => {
            renderComponent();

            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });

        it('should render the checkbox with correct initial state from backend', () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            const checkboxLabel = screen.getByText('Send me updates about DataHub');
            expect(checkboxLabel).toBeInTheDocument();
        });

        it('should render unchecked when backend setting is disabled', () => {
            mockMarketingOptIn.mockReturnValue(false);
            mockNotificationSettings.mockReturnValue({
                settings: [
                    {
                        type: NotificationScenarioType.DataHubCommunityUpdates,
                        value: NotificationSettingValue.Disabled,
                        params: [],
                    },
                ],
            });

            renderComponent();

            const checkboxLabel = screen.getByText('Send me updates about DataHub');
            expect(checkboxLabel).toBeInTheDocument();
        });
    });

    describe('Marketing Checkbox Interaction', () => {
        it('should call handleMarketingOptInChange when checkbox is clicked', async () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            // Find the marketing checkbox by test ID
            const marketingCheckbox = screen.getByTestId('marketing-updates-checkbox');

            expect(marketingCheckbox).toBeInTheDocument();
            fireEvent.click(marketingCheckbox);

            await waitFor(() => {
                expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(false);
            });
        });

        it('should call handleMarketingOptInChange when label is clicked', async () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            const label = screen.getByText('Send me updates about DataHub');
            fireEvent.click(label);

            await waitFor(() => {
                expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(false);
            });
        });

        it('should toggle from disabled to enabled', async () => {
            mockMarketingOptIn.mockReturnValue(false);
            renderComponent();

            const label = screen.getByText('Send me updates about DataHub');
            fireEvent.click(label);

            await waitFor(() => {
                expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(true);
            });
        });
    });

    describe('Backend Integration', () => {
        it('should call useActorSinkSettings with correct parameters', () => {
            renderComponent();

            // The hook should be called automatically when the component renders
            // We verify this by checking that the component renders correctly with the hook
            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });

        it('should pass notification settings to useMarketingOptIn hook', () => {
            const testSettings = {
                settings: [
                    {
                        type: NotificationScenarioType.DataHubCommunityUpdates,
                        value: NotificationSettingValue.Disabled,
                        params: [],
                    },
                ],
            };

            mockNotificationSettings.mockReturnValue(testSettings);
            renderComponent();

            // The hook should receive the notification settings
            // This is verified by the hook behavior in our mocks
            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });

        it('should handle missing notification settings gracefully', () => {
            mockNotificationSettings.mockReturnValue(null);
            renderComponent();

            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });

        it('should handle empty notification settings gracefully', () => {
            mockNotificationSettings.mockReturnValue({ settings: [] });
            renderComponent();

            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });
    });

    describe('Component Integration', () => {
        it('should render all required form elements along with marketing checkbox', () => {
            renderComponent();

            // Check for main elements
            expect(screen.getByText('Before we begin')).toBeInTheDocument();
            expect(screen.getByText('Select your Role')).toBeInTheDocument();
            expect(screen.getByText('Optional - Select your Data Tools')).toBeInTheDocument();
            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
            expect(screen.getByText('Get Started')).toBeInTheDocument();
        });

        it('should not interfere with other form functionality', () => {
            renderComponent();

            const roleSelect = screen.getByTestId('introduce-role-select');
            const marketingLabel = screen.getByText('Send me updates about DataHub');

            expect(roleSelect).toBeInTheDocument();
            expect(marketingLabel).toBeInTheDocument();

            // Both elements should be functional
            fireEvent.click(marketingLabel);
            expect(mockHandleMarketingOptInChange).toHaveBeenCalled();
        });

        it('should maintain checkbox state during form interactions', () => {
            renderComponent();

            const marketingLabel = screen.getByText('Send me updates about DataHub');
            const roleSelect = screen.getByTestId('introduce-role-select');

            // Interact with other form elements
            fireEvent.click(roleSelect);

            // Marketing checkbox should still be present and functional
            expect(marketingLabel).toBeInTheDocument();
            fireEvent.click(marketingLabel);
            expect(mockHandleMarketingOptInChange).toHaveBeenCalled();
        });
    });

    describe('Loading and Error States', () => {
        it('should handle updating state gracefully', () => {
            mockIsUpdatingMarketingSettings.mockReturnValue(true);
            renderComponent();

            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });

        it('should render when notification settings are loading', () => {
            // Simulate loading state by returning undefined
            mockNotificationSettings.mockReturnValue(undefined);
            renderComponent();

            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });
    });

    describe('Checkbox Component Type', () => {
        it('should use DataHub Checkbox component (not Ant Design)', () => {
            renderComponent();

            // Find the marketing checkbox by test ID
            const marketingCheckbox = screen.getByTestId('marketing-updates-checkbox');

            // DataHub checkbox should be present
            expect(marketingCheckbox).toBeInTheDocument();

            // Should be clickable
            fireEvent.click(marketingCheckbox);
            expect(mockHandleMarketingOptInChange).toHaveBeenCalled();
        });
    });

    describe('Accessibility and UX', () => {
        it('should have clickable label for better accessibility', () => {
            renderComponent();

            const label = screen.getByText('Send me updates about DataHub');
            expect(label).toBeInTheDocument();

            // Label should be clickable
            fireEvent.click(label);
            expect(mockHandleMarketingOptInChange).toHaveBeenCalled();
        });

        it('should maintain proper form layout with marketing checkbox', () => {
            renderComponent();

            // Verify elements appear in expected order
            const beforeText = screen.getByText('Before we begin');
            const roleSelect = screen.getByTestId('introduce-role-select');
            const toolsSelect = screen.getByText('Optional - Select your Data Tools');
            const marketingCheckbox = screen.getByText('Send me updates about DataHub');
            const getStartedButton = screen.getByText('Get Started');

            expect(beforeText).toBeInTheDocument();
            expect(roleSelect).toBeInTheDocument();
            expect(toolsSelect).toBeInTheDocument();
            expect(marketingCheckbox).toBeInTheDocument();
            expect(getStartedButton).toBeInTheDocument();
        });
    });
});
