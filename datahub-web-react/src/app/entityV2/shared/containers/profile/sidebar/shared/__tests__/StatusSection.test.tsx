import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
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

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)';
const TIME = 1700000000000;

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
                        dataNotCombinedWithSiblings: null,
                    }}
                >
                    <StatusSection />
                </EntityContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('StatusSection', () => {
    it.each([
        ['only an operation exists', { properties: {}, operations: [{ lastUpdatedTimestamp: TIME }] }],
        ['only lastModified exists', { properties: { lastModified: { time: TIME } }, operations: [] }],
        ['operations failed to load', { properties: { lastModified: { time: TIME } }, operations: null }],
    ])('shows Last Updated when %s', (_, data) => {
        const { getByText } = renderSection({ urn: DATASET_URN, type: EntityType.Dataset, ...data });
        expect(getByText('Last Updated:')).toBeInTheDocument();
    });
});
