/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import * as useUserContextModule from '@app/context/useUserContext';
import { useIsProductAnnouncementEnabled, useIsProductAnnouncementVisible } from '@app/shared/product/update/hooks';
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

    describe('useIsProductAnnouncementVisible', () => {
        beforeEach(() => {
            vi.spyOn(useUserContextModule, 'useUserContext').mockReturnValue({
                user: { urn: 'urn:li:user:123' },
            } as any);
        });

        it('returns visible=false when step state exists', async () => {
            const { result } = renderHook(() => useIsProductAnnouncementVisible(TEST_UPDATE.id), {
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
            const { result } = renderHook(() => useIsProductAnnouncementVisible(TEST_UPDATE.id), {
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
            const { result } = renderHook(() => useIsProductAnnouncementVisible(TEST_UPDATE.id), {
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
