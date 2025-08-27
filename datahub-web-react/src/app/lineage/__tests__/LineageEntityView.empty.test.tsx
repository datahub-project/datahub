import { render } from '@testing-library/react';
import React from 'react';

import LineageEntityView from '@app/lineage/manage/LineageEntityView';
import { dataset1 } from '@src/Mocks';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

const mockEntityRegistry = getTestEntityRegistry();
vi.mock('../../useEntityRegistry', () => ({
    useEntityRegistry: () => mockEntityRegistry,
}));
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => {
        return {};
    },
}));
describe('LineageEntityViewEmpty', () => {
    it('should render an entity properly in LineageEntityView if no containers', () => {
        const { getByTestId, getByText } = render(<LineageEntityView entity={dataset1} />);
        // expect platform logo, platform name, divider, entity type, and display name
        expect(getByTestId('platform-logo')).toBeInTheDocument();
        expect(getByText(dataset1.platform.name)).toBeInTheDocument();
        expect(getByTestId('divider')).toBeInTheDocument();
        expect(getByText('Dataset')).toBeInTheDocument();
        expect(getByText(dataset1.properties.name)).toBeInTheDocument();
    });
});
