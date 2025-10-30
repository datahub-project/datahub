import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import * as useUserContextModule from '@app/context/useUserContext';
import {
    useGetLatestProductAnnouncementData,
    useIsProductAnnouncementEnabled,
    useIsProductAnnouncementVisible,
} from '@app/shared/product/update/hooks';
import { latestUpdate } from '@app/shared/product/update/latestUpdate';
import * as useAppConfigModule from '@app/useAppConfig';

import { BatchGetStepStatesDocument } from '@graphql/step.generated';

const STEP_ID = 'urn:li:user:123-product_updates-test';
const TEST_UPDATE = {
    enabled: true,
    id: 'test',
    title: "What's New In DataHub",
    description: 'Explore version v0.3.12',
    ctaText: 'Read updates',
    ctaLink: 'https://docs.yourapp.com/releases/v0.3.12',
};

const BATCH_GET_STEP_STATES_MOCK_PRESENT = {
    request: {
        query: BatchGetStepStatesDocument,
        variables: {
            input: {
                ids: [STEP_ID],
            },
        },
    },
    result: {
        data: {
            batchGetStepStates: {
                results: [
                    {
                        id: STEP_ID,
                        properties: [],
                    },
                ],
            },
        },
    },
};

const BATCH_GET_STEP_STATES_MOCK_NOT_PRESENT = {
    request: {
        query: BatchGetStepStatesDocument,
        variables: {
            input: {
                ids: [STEP_ID],
            },
        },
    },
    result: {
        data: {
            batchGetStepStates: {
                results: [],
            },
        },
    },
};

const BATCH_GET_STEP_STATES_MOCK_LOADING = {
    request: {
        query: BatchGetStepStatesDocument,
        variables: {
            input: {
                ids: [STEP_ID],
            },
        },
    },
    result: {
        data: undefined,
        loading: true,
    },
};

describe('product update hooks', () => {
    describe('useIsProductAnnouncementEnabled', () => {
        it('returns true when feature flag is enabled', () => {
            vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue({
                config: {
                    featureFlags: {
                        showProductUpdates: true,
                    },
                },
            } as any);

            const { result } = renderHook(() => useIsProductAnnouncementEnabled());
            expect(result.current).toBe(true);
        });

        it('returns false when feature flag is disabled', () => {
            vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue({
                config: {
                    featureFlags: {
                        showProductUpdates: false,
                    },
                },
            } as any);

            const { result } = renderHook(() => useIsProductAnnouncementEnabled());
            expect(result.current).toBe(false);
        });
    });

    describe('useGetLatestProductAnnouncementData', () => {
        it('returns template with placeholders when no version is available', () => {
            vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue({
                config: {
                    appVersion: undefined,
                },
            } as any);

            const { result } = renderHook(() => useGetLatestProductAnnouncementData());
            expect(result.current).toBe(latestUpdate);
            expect(result.current.id).toBe('{{VERSION}}');
            expect(result.current.description).toBe('Explore version {{VERSION}}');
        });

        it('injects version into placeholders when appVersion is available', () => {
            vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue({
                config: {
                    appVersion: 'v1.3.0',
                },
            } as any);

            const { result } = renderHook(() => useGetLatestProductAnnouncementData());
            expect(result.current.id).toBe('v1.3.0');
            expect(result.current.description).toBe('Explore version v1.3.0');
            expect(result.current.ctaLink).toBe('https://docs.datahub.com/docs/releases#v1-3-0');
        });

        it('handles version format correctly for URL conversion', () => {
            vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue({
                config: {
                    appVersion: 'v1.4.5',
                },
            } as any);

            const { result } = renderHook(() => useGetLatestProductAnnouncementData());
            expect(result.current.ctaLink).toBe('https://docs.datahub.com/docs/releases#v1-4-5');
        });
    });

    describe('useIsProductAnnouncementVisible', () => {
        beforeEach(() => {
            vi.spyOn(useUserContextModule, 'useUserContext').mockReturnValue({
                user: { urn: 'urn:li:user:123' },
            } as any);
        });

        it('returns visible=false when step state exists', async () => {
            const { result } = renderHook(() => useIsProductAnnouncementVisible(TEST_UPDATE), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={[BATCH_GET_STEP_STATES_MOCK_PRESENT]} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            await waitFor(() => {
                expect(result.current.visible).toBe(false);
            });
        });

        it('returns visible=true when step state does not exist', async () => {
            const { result } = renderHook(() => useIsProductAnnouncementVisible(TEST_UPDATE), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={[BATCH_GET_STEP_STATES_MOCK_NOT_PRESENT]} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            await waitFor(() => {
                expect(result.current.visible).toBe(true);
            });
        });

        it('returns visible=false when query is loading', () => {
            const { result } = renderHook(() => useIsProductAnnouncementVisible(TEST_UPDATE), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={[BATCH_GET_STEP_STATES_MOCK_LOADING]} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            expect(result.current.visible).toBe(false);
        });
    });
});
