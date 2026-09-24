import { render } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import EntityHeader from '@app/previewV2/EntityHeader';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { FabricType } from '@types';

// EntityHeader is entity-agnostic: it renders whatever `environment` it's given, so a single
// mocked value stands in for both the dataset (origin) and container (properties.origin) sources —
// getEntityEnvironment itself is covered by getEntityEnvironment.test.ts.
const mockUseAppConfig = vi.fn();
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => mockUseAppConfig(),
}));

function renderHeader(environment: FabricType | null, showEnvironmentBadge: boolean) {
    mockUseAppConfig.mockReturnValue({
        config: { visualConfig: { showEnvironmentBadge } },
        loaded: false,
    });
    return render(
        <CustomThemeProvider>
            <MemoryRouter>
                <EntityHeader
                    name="events"
                    url="/dataset/urn"
                    urn="urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)"
                    deprecation={null}
                    health={undefined}
                    environment={environment}
                />
            </MemoryRouter>
        </CustomThemeProvider>,
    );
}

describe('EntityHeader environment badge', () => {
    it('shows the env pill for a dataset with origin=PROD when the toggle is on', () => {
        const { getByText } = renderHeader(FabricType.Prod, true);
        expect(getByText('PROD')).toBeInTheDocument();
    });

    it('hides the env pill when the toggle is off', () => {
        const { queryByText } = renderHeader(FabricType.Prod, false);
        expect(queryByText('PROD')).toBeNull();
    });

    it('shows the env pill for a container with properties.origin=DEV when the toggle is on', () => {
        const { getByText } = renderHeader(FabricType.Dev, true);
        expect(getByText('DEV')).toBeInTheDocument();
    });

    it('hides the env pill when there is no resolved environment', () => {
        const { queryByText } = renderHeader(null, true);
        expect(queryByText('PROD')).toBeNull();
        expect(queryByText('DEV')).toBeNull();
    });
});
