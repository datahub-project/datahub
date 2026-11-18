import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { SidebarParentApplicationSection } from '@app/entityV2/application/SidebarParentApplicationSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import {
    useBatchSetApplicationMutation,
    useGetApplicationsListLazyQuery,
    useMoveApplicationMutation,
} from '@graphql/application.generated';
import { EntityType } from '@types';

vi.mock('@graphql/application.generated');

const APP_URN = 'urn:li:application:child';
const PARENT_URN = 'urn:li:application:parent';

const mocks = vi.hoisted(() => ({
    entityData: {} as Record<string, unknown>,
    refetch: vi.fn(),
}));

vi.mock('@app/entityV2/shared/EntityContext', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/entityV2/shared/EntityContext')>()),
    useEntityData: () => ({ urn: APP_URN, entityType: EntityType.Application, entityData: mocks.entityData }),
    useMutationUrn: () => APP_URN,
    useRefetch: () => mocks.refetch,
}));

// SimpleSelect only mounts its trigger once IntersectionObserver reports it as visible.
beforeAll(() => {
    vi.stubGlobal(
        'IntersectionObserver',
        class {
            private readonly callback: IntersectionObserverCallback;

            constructor(callback: IntersectionObserverCallback) {
                this.callback = callback;
            }

            observe(target: Element) {
                this.callback(
                    [{ isIntersecting: true, target } as IntersectionObserverEntry],
                    this as unknown as IntersectionObserver,
                );
            }

            unobserve() {}

            disconnect() {}

            takeRecords() {
                return [];
            }
        },
    );
});

const searchResult = (urn: string, name: string) => ({
    entity: { __typename: 'Application' as const, urn, properties: { name } },
});

const mockMoveApplication = vi.fn();

const renderSection = () =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <SidebarParentApplicationSection />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('SidebarParentApplicationSection', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockMoveApplication.mockResolvedValue({});
        vi.mocked(useMoveApplicationMutation).mockReturnValue([mockMoveApplication, {} as any]);
        vi.mocked(useBatchSetApplicationMutation).mockReturnValue([vi.fn(), {} as any]);
        vi.mocked(useGetApplicationsListLazyQuery).mockReturnValue([
            vi.fn(),
            {
                data: {
                    searchAcrossEntities: {
                        searchResults: [searchResult(APP_URN, 'Child'), searchResult(PARENT_URN, 'Parent')],
                    },
                },
                loading: false,
            } as any,
        ]);
        mocks.entityData = { privileges: { canEditProperties: true }, parentApplications: { applications: [] } };
    });

    it('sets the chosen application as parent, and does not offer the application itself', async () => {
        renderSection();

        fireEvent.click(screen.getByTestId('set-parent-application-button'));
        fireEvent.click(screen.getByTestId('application-select-base'));
        expect(screen.queryByTestId(`application-option-${APP_URN}`)).not.toBeInTheDocument();
        fireEvent.click(await screen.findByTestId(`application-option-${PARENT_URN}`));
        fireEvent.click(screen.getByTestId('set-application-modal-add-button'));

        await waitFor(() =>
            expect(mockMoveApplication).toHaveBeenCalledWith({
                variables: { input: { resourceUrn: APP_URN, parentApplication: PARENT_URN } },
            }),
        );
    });

    it('clears the parent', async () => {
        mocks.entityData = {
            privileges: { canEditProperties: true },
            parentApplications: {
                applications: [{ urn: PARENT_URN, type: EntityType.Application, properties: { name: 'Parent' } }],
            },
        };
        renderSection();

        expect(screen.getByText('Parent')).toBeInTheDocument();
        fireEvent.click(screen.getByTestId('remove-parent-application-button'));

        await waitFor(() =>
            expect(mockMoveApplication).toHaveBeenCalledWith({
                variables: { input: { resourceUrn: APP_URN, parentApplication: null } },
            }),
        );
    });
});
