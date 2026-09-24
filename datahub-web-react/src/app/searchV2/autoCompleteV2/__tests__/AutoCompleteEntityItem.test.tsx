import { render } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { Entity, EntityType, FabricType } from '@src/types.generated';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

const mockUseAppConfig = vi.fn();
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => mockUseAppConfig(),
}));

const PLATFORM = {
    urn: 'urn:li:dataPlatform:mysql',
    type: EntityType.DataPlatform,
    name: 'mysql',
    properties: null,
};

const datasetEntity = {
    urn: 'urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)',
    type: EntityType.Dataset,
    name: 'events',
    origin: FabricType.Qa,
    properties: { name: 'events' },
    platform: PLATFORM,
} as unknown as Entity;

const containerEntity = {
    urn: 'urn:li:container:abc',
    type: EntityType.Container,
    properties: { name: 'my_schema', origin: FabricType.Dev },
    platform: PLATFORM,
} as unknown as Entity;

function renderItem(entity: Entity, showEnvironmentBadge: boolean) {
    mockUseAppConfig.mockReturnValue({
        config: { visualConfig: { showEnvironmentBadge } },
        loaded: false,
    });
    return render(
        <CustomThemeProvider>
            <MemoryRouter>
                <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                    <AutoCompleteEntityItem entity={entity} customIconRenderer={() => null} hideSubtitle hideMatches />
                </EntityRegistryContext.Provider>
            </MemoryRouter>
        </CustomThemeProvider>,
    );
}

describe('AutoCompleteEntityItem environment badge', () => {
    it('shows the env pill for a dataset with origin=QA when the toggle is on', () => {
        const { getByText } = renderItem(datasetEntity, true);
        expect(getByText('QA')).toBeInTheDocument();
    });

    it('shows the env pill for a container with properties.origin=DEV when the toggle is on', () => {
        const { getByText } = renderItem(containerEntity, true);
        expect(getByText('DEV')).toBeInTheDocument();
    });

    it('hides the env pill when the toggle is off', () => {
        const { queryByText } = renderItem(datasetEntity, false);
        expect(queryByText('QA')).toBeNull();
    });
});
