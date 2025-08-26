import { MockedProvider } from '@apollo/client/testing';
import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import React from 'react';
import { vi } from 'vitest';

import { useMarketingOptIn } from '@app/settingsV2/personal/notifications/useMarketingOptIn';

import { UpdateUserNotificationSettingsDocument } from '@graphql/settings.generated';
import { NotificationScenarioType, NotificationSetting, NotificationSettingValue } from '@types';

// Mock console.error to avoid noise in tests
const mockConsoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

// Mock Ant Design message
vi.mock('antd', () => ({
    message: {
        destroy: vi.fn(),
        success: vi.fn(),
        error: vi.fn(),
    },
}));

describe('useMarketingOptIn', () => {
    const mockRefetch = vi.fn();
    const mockOnError = vi.fn();
    const mockOnSuccess = vi.fn();

    const createMockSetting = (value: NotificationSettingValue, params: any[] = []): NotificationSetting => ({
        type: NotificationScenarioType.DataHubCommunityUpdates,
        value,
        params,
    });

    // Create GraphQL mocks for the updateUserNotificationSettings mutation
    const createUpdateUserNotificationSettingsMock = (inputSettings: any[]) => ({
        request: {
            query: UpdateUserNotificationSettingsDocument,
            variables: {
                input: {
                    notificationSettings: {
                        settings: inputSettings,
                    },
                },
            },
        },
        result: {
            data: {
                updateUserNotificationSettings: {
                    sinkTypes: [],
                    slackSettings: null,
                    emailSettings: null,
                    settings: inputSettings,
                },
            },
        },
    });

    const createWrapper = (mocks: any[] = []) => {
        return ({ children }: { children?: React.ReactNode }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                {children}
            </MockedProvider>
        );
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockConsoleError.mockClear();
        vi.mocked(message.destroy).mockClear();
        vi.mocked(message.success).mockClear();
        vi.mocked(message.error).mockClear();
    });

    afterAll(() => {
        mockConsoleError.mockRestore();
    });

    describe('Initialization', () => {
        it('should initialize with default values', () => {
            const { result } = renderHook(() => useMarketingOptIn({}), {
                wrapper: createWrapper(),
            });

            expect(result.current.marketingOptIn).toBe(true);
            expect(result.current.isUpdatingMarketingSettings).toBe(false);
            expect(typeof result.current.handleMarketingOptInChange).toBe('function');
        });

        it('should initialize with marketingOptIn true when no settings provided', () => {
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: undefined,
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
        });

        it('should initialize with marketingOptIn true when empty settings provided', () => {
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [] },
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
        });
    });

    describe('State Management Based on Settings', () => {
        it('should set marketingOptIn to true when community updates setting is enabled', () => {
            const enabledSetting = createMockSetting(NotificationSettingValue.Enabled);
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [enabledSetting] },
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
        });

        it('should set marketingOptIn to false when community updates setting is disabled', () => {
            const disabledSetting = createMockSetting(NotificationSettingValue.Disabled);
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [disabledSetting] },
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(false);
        });

        it('should default to true when community updates setting is not found', () => {
            const otherSetting = {
                type: NotificationScenarioType.NewProposal,
                value: NotificationSettingValue.Enabled,
                params: [],
            };
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [otherSetting] },
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
        });
    });

    describe('Callback Handling', () => {
        it('should work without optional callbacks', () => {
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [] },
                        // No callbacks provided
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
            expect(result.current.isUpdatingMarketingSettings).toBe(false);
            expect(typeof result.current.handleMarketingOptInChange).toBe('function');
        });

        it('should accept all callback props', () => {
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [] },
                        refetchNotificationSettings: mockRefetch,
                        onSuccess: mockOnSuccess,
                        onError: mockOnError,
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
            expect(result.current.isUpdatingMarketingSettings).toBe(false);
            expect(typeof result.current.handleMarketingOptInChange).toBe('function');
        });
    });

    describe('Edge Cases', () => {
        it('should handle settings with no params', () => {
            const settingWithoutParams = {
                type: NotificationScenarioType.NewProposal,
                value: NotificationSettingValue.Enabled,
                // No params property
            };

            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [settingWithoutParams] },
                        refetchNotificationSettings: mockRefetch,
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
        });

        it('should handle empty params array', () => {
            const settingWithEmptyParams = {
                type: NotificationScenarioType.NewProposal,
                value: NotificationSettingValue.Enabled,
                params: [],
            };

            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [settingWithEmptyParams] },
                        refetchNotificationSettings: mockRefetch,
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            expect(result.current.marketingOptIn).toBe(true);
        });

        it('should handle updating existing community updates setting', () => {
            const existingCommunitySetting = createMockSetting(NotificationSettingValue.Disabled);
            const otherSetting = {
                type: NotificationScenarioType.NewProposal,
                value: NotificationSettingValue.Enabled,
                params: [],
            };

            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [existingCommunitySetting, otherSetting] },
                        refetchNotificationSettings: mockRefetch,
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            // Should start as false due to disabled setting
            expect(result.current.marketingOptIn).toBe(false);
        });
    });

    describe('State Updates', () => {
        it('should update state when actorNotificationSettings change', () => {
            const initialSettings = { settings: [] as NotificationSetting[] };
            const { result, rerender } = renderHook(
                (props: { settings: { settings: NotificationSetting[] } }) =>
                    useMarketingOptIn({ actorNotificationSettings: props.settings }),
                {
                    wrapper: createWrapper(),
                    initialProps: { settings: initialSettings },
                },
            );

            // Initially true (default)
            expect(result.current.marketingOptIn).toBe(true);

            // Update with disabled setting
            const disabledSetting = createMockSetting(NotificationSettingValue.Disabled);
            rerender({ settings: { settings: [disabledSetting] } });

            expect(result.current.marketingOptIn).toBe(false);

            // Update with enabled setting
            const enabledSetting = createMockSetting(NotificationSettingValue.Enabled);
            rerender({ settings: { settings: [enabledSetting] } });

            expect(result.current.marketingOptIn).toBe(true);
        });
    });

    describe('Function Calls', () => {
        it('should call handleMarketingOptInChange and update state immediately', () => {
            const expectedSettings = [
                {
                    type: NotificationScenarioType.DataHubCommunityUpdates,
                    value: NotificationSettingValue.Disabled,
                },
            ];

            const mock = createUpdateUserNotificationSettingsMock(expectedSettings);
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [] },
                        refetchNotificationSettings: mockRefetch,
                        onSuccess: mockOnSuccess,
                        onError: mockOnError,
                    }),
                {
                    wrapper: createWrapper([mock]),
                },
            );

            // Initially true (default)
            expect(result.current.marketingOptIn).toBe(true);
            expect(result.current.isUpdatingMarketingSettings).toBe(false);

            // Call the function to toggle to false
            act(() => {
                result.current.handleMarketingOptInChange(false);
            });

            // Should immediately update state
            expect(result.current.marketingOptIn).toBe(false);
            expect(result.current.isUpdatingMarketingSettings).toBe(true);
        });

        it('should call handleMarketingOptInChange with true', () => {
            const disabledSetting = createMockSetting(NotificationSettingValue.Disabled);
            const expectedSettings = [
                {
                    type: NotificationScenarioType.DataHubCommunityUpdates,
                    value: NotificationSettingValue.Enabled,
                },
            ];

            const mock = createUpdateUserNotificationSettingsMock(expectedSettings);
            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [disabledSetting] },
                        refetchNotificationSettings: mockRefetch,
                        onSuccess: mockOnSuccess,
                        onError: mockOnError,
                    }),
                {
                    wrapper: createWrapper([mock]),
                },
            );

            // Initially false (from disabled setting)
            expect(result.current.marketingOptIn).toBe(false);
            expect(result.current.isUpdatingMarketingSettings).toBe(false);

            // Call the function to toggle to true
            act(() => {
                result.current.handleMarketingOptInChange(true);
            });

            // Should immediately update state
            expect(result.current.marketingOptIn).toBe(true);
            expect(result.current.isUpdatingMarketingSettings).toBe(true);
        });
    });

    describe('Settings Filtering', () => {
        it('should preserve other notification settings structure', () => {
            const otherSetting = {
                type: NotificationScenarioType.NewProposal,
                value: NotificationSettingValue.Enabled,
                params: [{ key: 'email.enabled', value: 'true' }],
            };

            const { result } = renderHook(
                () =>
                    useMarketingOptIn({
                        actorNotificationSettings: { settings: [otherSetting] },
                        refetchNotificationSettings: mockRefetch,
                    }),
                {
                    wrapper: createWrapper(),
                },
            );

            // Should work with other settings present
            expect(result.current.marketingOptIn).toBe(true);
            expect(result.current.isUpdatingMarketingSettings).toBe(false);
        });
    });
});
