import { render } from '@testing-library/react';
import React from 'react';
import { dataset1 } from '../../../Mocks';
import { getTestEntityRegistry } from '../../../utils/test-utils/TestPageContainer';
import LineageEntityView from '../manage/LineageEntityView';

const mockEntityRegistry = getTestEntityRegistry();
vi.mock('../../useEntityRegistry', () => ({
    useEntityRegistry: () => mockEntityRegistry,
}));

describe('LineageEntityView', () => {
    it('should render an entity properly in LineageEntityView', () => {
        const { getByTestId, getByText } = render(<LineageEntityView entity={dataset1} />);

        // expect platform logo, platform name, divider, entity type, and display name
        expect(getByTestId('platform-logo')).toBeInTheDocument();
        expect(getByText(dataset1.platform.name)).toBeInTheDocument();
        expect(getByTestId('divider')).toBeInTheDocument();
        expect(getByText('Dataset')).toBeInTheDocument();
        expect(getByText(dataset1.properties.name)).toBeInTheDocument();
    });

    it('should render the subtype name if it exists and not the type name', () => {
        const datasetWithSubtype = { ...dataset1, subTypes: { typeNames: ['view'] } };
        const { getByText, queryByText } = render(<LineageEntityView entity={datasetWithSubtype} />);

        expect(queryByText('Dataset')).not.toBeInTheDocument();
        expect(getByText('View')).toBeInTheDocument();
    });

    it('should not render a divider if there is no platform name', () => {
        const datasetNoPlatformName = {
            ...dataset1,
            platform: { ...dataset1.platform, name: '', properties: { displayName: '' } },
        };
        const { queryByTestId } = render(<LineageEntityView entity={datasetNoPlatformName} />);

        expect(queryByTestId('divider')).not.toBeInTheDocument();
    });
});
