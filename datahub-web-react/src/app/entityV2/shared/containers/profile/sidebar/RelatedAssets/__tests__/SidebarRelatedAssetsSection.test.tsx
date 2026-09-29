import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { SidebarRelatedAssetsSection } from '@app/entityV2/shared/containers/profile/sidebar/RelatedAssets/SidebarRelatedAssetsSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

// `vi.hoisted` is hoisted by vitest above all imports so the polyfill is in place before
// `checkAuthStatus()` runs at `checkAuthStatus.ts` module-load time — that path reaches
// `analytics.identify -> getMergedTrackingOptions -> localStorage.getItem`.
vi.hoisted(() => {
    const storage = new Map<string, string>();
    Object.defineProperty(globalThis, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key: string) => storage.get(key) ?? null,
            setItem: (key: string, value: string) => {
                storage.set(key, String(value));
            },
            removeItem: (key: string) => {
                storage.delete(key);
            },
            clear: () => storage.clear(),
            key: (index: number) => Array.from(storage.keys())[index] ?? null,
            get length() {
                return storage.size;
            },
        },
    });
});

const DOCUMENT_URN = 'urn:li:document:sidebar-related-assets-test';
const LINKED_DP_URN = 'urn:li:dataProduct:linked-dp';

const renderSection = (entityData: unknown) =>
    render(
        <MockedProvider addTypename={false}>
            <TestPageContainer initialEntries={[`/document/${DOCUMENT_URN}`]}>
                <EntityContext.Provider
                    value={{
                        urn: DOCUMENT_URN,
                        entityType: EntityType.Document,
                        // @ts-expect-error entityData is loose-typed across sidebar sections
                        entityData,
                        baseEntity: { document: entityData },
                        routeToTab: vi.fn(),
                        refetch: vi.fn(),
                        lineage: undefined,
                        loading: false,
                        dataNotCombinedWithSiblings: null,
                    }}
                >
                    <SidebarRelatedAssetsSection />
                </EntityContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );

describe('SidebarRelatedAssetsSection', () => {
    it('renders a row per related asset when entityData.info.relatedAssets has entries', () => {
        const entityData = {
            urn: DOCUMENT_URN,
            type: EntityType.Document,
            info: {
                relatedAssets: [
                    {
                        asset: {
                            urn: LINKED_DP_URN,
                            type: EntityType.DataProduct,
                        },
                    },
                ],
            },
        };

        const { getByText, queryByText } = renderSection(entityData);

        // Section heading renders
        expect(getByText('Related Assets')).toBeInTheDocument();
        // Empty-state message must not be shown when the array has entries
        expect(queryByText(/no related assets/i)).not.toBeInTheDocument();
    });

    it('renders the empty state when relatedAssets is an empty array', () => {
        const entityData = {
            urn: DOCUMENT_URN,
            type: EntityType.Document,
            info: {
                relatedAssets: [],
            },
        };

        const { getByText } = renderSection(entityData);

        expect(getByText('Related Assets')).toBeInTheDocument();
        expect(getByText(/no related assets/i)).toBeInTheDocument();
    });

    it('renders the empty state when info is undefined', () => {
        const entityData = {
            urn: DOCUMENT_URN,
            type: EntityType.Document,
        };

        const { getByText } = renderSection(entityData);

        expect(getByText('Related Assets')).toBeInTheDocument();
        expect(getByText(/no related assets/i)).toBeInTheDocument();
    });
});
