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

const STEP_ID = 'product-updates-test';
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
        it('returns latest update object', () => {
            const { result } = renderHook(() => useGetLatestProductAnnouncementData());
            expect(result.current).toBe(latestUpdate);
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
