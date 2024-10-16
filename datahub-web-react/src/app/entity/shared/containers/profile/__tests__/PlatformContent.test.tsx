import React from 'react';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import PlatformContentView, { getParentContainerNames } from '../header/PlatformContent/PlatformContentView';
import { EntityType } from '../../../../../../types.generated';
import { container1, container2 } from '../../../../../../Mocks';

vi.mock('../../../../../useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: vi.fn(() => 'test'),
        getDisplayName: vi.fn(() => 'database1'),
    }),
}));

describe('PlatformContent', () => {
    const defaultProps = {
        platformName: 'mysql',
        entityLogoComponent: <></>,
        typeIcon: <></>,
        entityType: EntityType.Dataset,
        parentContainers: [],
        parentContainersRef: React.createRef<HTMLDivElement>(),
        areContainersTruncated: false,
    };

    it('should not render any containers if there are none in parentContainers', () => {
        const { queryAllByTestId } = render(<PlatformContentView {...defaultProps} />);

        expect(queryAllByTestId('container')).toHaveLength(0);
        expect(queryAllByTestId('right-arrow')).toHaveLength(0);
    });

    it('should render a direct parent container correctly when there is only one container', () => {
        const { queryAllByTestId, getByText } = render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} parentContainers={[container1]} />
            </BrowserRouter>,
        );

        expect(queryAllByTestId('container')).toHaveLength(1);
        expect(queryAllByTestId('right-arrow')).toHaveLength(1);
        expect(getByText('database1')).toBeInTheDocument();
    });

    it('should render all parent containers properly', () => {
        const { queryAllByTestId } = render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} parentContainers={[container1, container2]} />
            </BrowserRouter>,
        );

        expect(queryAllByTestId('container')).toHaveLength(2);
        expect(queryAllByTestId('right-arrow')).toHaveLength(2);
    });

    it('should render the correct number of right arrows with no containers and an instanceId', () => {
        render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} instanceId="mysql1" />
            </BrowserRouter>,
        );
        expect(screen.queryAllByTestId('right-arrow')).toHaveLength(1);
    });

    it('should render the correct number of right arrows with multiple containers and an instanceId', () => {
        render(
            <BrowserRouter>
                <PlatformContentView
                    {...defaultProps}
                    instanceId="mysql1"
                    parentContainers={[container1, container2]}
                />
            </BrowserRouter>,
        );
        expect(screen.queryAllByTestId('right-arrow')).toHaveLength(3);
    });

    it('should render the correct number of right arrows with one containers and an instanceId', () => {
        render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} instanceId="mysql1" parentContainers={[container2]} />
            </BrowserRouter>,
        );
        expect(screen.queryAllByTestId('right-arrow')).toHaveLength(2);
    });

    it('should render the correct number of right arrows with multiple containers and no instanceId', () => {
        render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} parentContainers={[container1, container2]} />
            </BrowserRouter>,
        );
        expect(screen.queryAllByTestId('right-arrow')).toHaveLength(2);
    });

    it('should render the correct number of right arrows with one container and no instanceId', () => {
        render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} parentContainers={[container1]} />
            </BrowserRouter>,
        );
        expect(screen.queryAllByTestId('right-arrow')).toHaveLength(1);
    });

    it('should render the correct number of right arrows with no containers and no instanceID', () => {
        render(
            <BrowserRouter>
                <PlatformContentView {...defaultProps} parentContainers={[]} />
            </BrowserRouter>,
        );
        expect(screen.queryAllByTestId('right-arrow')).toHaveLength(0);
    });
});

describe('getParentContainerNames', () => {
    it('should return an empty string if there are no parent containers', () => {
        const parentContainerNames = getParentContainerNames([]);
        expect(parentContainerNames).toBe('');
    });

    it('should return the name of the parent container if there is only one', () => {
        const parentContainerNames = getParentContainerNames([container1]);
        expect(parentContainerNames).toBe(container1.properties?.name);
    });

    it('should return the names of the parents in reverse order, separated by cright arrows if there are multiple without manipulating the original array', () => {
        const parentContainers = [container1, container2];
        const parentContainersCopy = [...parentContainers];
        const parentContainerNames = getParentContainerNames(parentContainers);

        expect(parentContainerNames).toBe(`${container2.properties?.name} > ${container1.properties?.name}`);
        expect(parentContainers).toMatchObject(parentContainersCopy);
    });
});
