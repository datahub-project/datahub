import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { SidebarSiblingsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSiblingsSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

// localStorage polyfill required before checkAuthStatus runs at module-load time
vi.hoisted(() => {
    const storage = new Map<string, string>();
    Object.defineProperty(globalThis, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key: string) => storage.get(key) ?? null,
            setItem: (key: string, value: string) => storage.set(key, String(value)),
            removeItem: (key: string) => storage.delete(key),
            clear: () => storage.clear(),
            key: (index: number) => Array.from(storage.keys())[index] ?? null,
            get length() {
                return storage.size;
            },
        },
    });
});

const mockShowSeparateSiblings = vi.fn(() => false);

vi.mock('@src/app/useAppConfig', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@src/app/useAppConfig')>();
    return {
        ...actual,
        useIsShowSeparateSiblingsEnabled: () => mockShowSeparateSiblings(),
    };
});

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:iceberg,test.table,PROD)';
const SIBLING_URN = 'urn:li:dataset:(urn:li:dataPlatform:Feature%20Platform,featureset.test,PROD)';

const siblingEntity = {
    urn: SIBLING_URN,
    type: EntityType.Dataset,
    exists: true,
    name: 'featureset.test',
    platform: { name: 'Feature Platform' },
};

const entityDataWithSiblings = {
    urn: DATASET_URN,
    type: EntityType.Dataset,
    exists: true,
    siblingsSearch: {
        total: 1,
        count: 1,
        searchResults: [{ entity: siblingEntity }],
    },
};

const entityDataWithoutSiblings = {
    urn: DATASET_URN,
    type: EntityType.Dataset,
    exists: true,
    siblingsSearch: {
        total: 0,
        count: 0,
        searchResults: [],
    },
};

const dataNotCombinedWithSiblings = {
    dataset: {
        urn: DATASET_URN,
        type: EntityType.Dataset,
        exists: true,
        siblings: null,
        siblingsSearch: null,
        siblingPlatforms: null,
    },
};

function renderSection(entityData: unknown) {
    return render(
        <MockedProvider addTypename={false}>
            <TestPageContainer initialEntries={[`/dataset/${DATASET_URN}`]}>
                <EntityContext.Provider
                    value={{
                        urn: DATASET_URN,
                        entityType: EntityType.Dataset,
                        // @ts-expect-error entityData is loose-typed across sidebar sections
                        entityData,
                        baseEntity: { dataset: entityData },
                        routeToTab: vi.fn(),
                        refetch: vi.fn(),
                        lineage: undefined,
                        loading: false,
                        dataNotCombinedWithSiblings,
                    }}
                >
                    <SidebarSiblingsSection />
                </EntityContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SidebarSiblingsSection', () => {
    beforeEach(() => {
        mockShowSeparateSiblings.mockReturnValue(false);
    });

    it('renders "Composed of" when siblings exist and showSeparateSiblings is false', () => {
        mockShowSeparateSiblings.mockReturnValue(false);
        const { getByText } = renderSection(entityDataWithSiblings);
        expect(getByText('Composed of')).toBeInTheDocument();
    });

    it('renders "Composed of" when siblings exist and showSeparateSiblings is true', () => {
        // Regression test: when SHOW_SEPARATE_SIBLINGS=true, the getDataset query previously
        // skipped siblingsSearch entirely via @skip(if: $skipSiblingsSearch), causing this
        // section to never mount. The fix removes @skip from getDataset so siblingsSearch
        // is always fetched on the entity profile page.
        mockShowSeparateSiblings.mockReturnValue(true);
        const { getByText } = renderSection(entityDataWithSiblings);
        expect(getByText('Composed of')).toBeInTheDocument();
    });

    it('renders nothing when there are no siblings', () => {
        const { container } = renderSection(entityDataWithoutSiblings);
        expect(container.firstChild).toBeNull();
    });

    it('renders nothing when entity data is absent', () => {
        const { container } = renderSection(null);
        expect(container.firstChild).toBeNull();
    });
});
