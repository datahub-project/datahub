import React from 'react';
import { render } from '@testing-library/react';
import { Zoom } from '@vx/zoom';

import { dataset3WithLineage, dataset4WithLineage, dataset5WithLineage, dataset6WithLineage } from '../../../Mocks';
import { Direction, FetchedEntities } from '../types';
import constructTree from '../utils/constructTree';
import LineageTree from '../LineageTree';
import extendAsyncEntities from '../utils/extendAsyncEntities';
import TestPageContainer, { getTestEntityRegistry } from '../../../utils/test-utils/TestPageContainer';
import { EntityType } from '../../../types.generated';

const margin = { top: 10, left: 280, right: 280, bottom: 10 };
const [windowWidth, windowHeight] = [1000, 500];

const height = windowHeight - 125;
const width = windowWidth;
const yMax = height - margin.top - margin.bottom;
const initialTransform = {
    scaleX: 2 / 3,
    scaleY: 2 / 3,
    translateX: width / 2,
    translateY: 0,
    skewX: 0,
    skewY: 0,
};

const testEntityRegistry = getTestEntityRegistry();

describe('LineageTree', () => {
    it('renders a tree with many layers', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity, type: EntityType.Dataset },
                    entry.fullyFetched,
                ),
            {} as FetchedEntities,
        );

        const downstreamData = constructTree(
            { entity: dataset3WithLineage, type: EntityType.Dataset },
            mockFetchedEntities,
            Direction.Downstream,
            testEntityRegistry,
        );
        const upstreamData = constructTree(
            { entity: dataset3WithLineage, type: EntityType.Dataset },
            mockFetchedEntities,
            Direction.Upstream,
            testEntityRegistry,
        );

        const { getByTestId } = render(
            <TestPageContainer>
                <Zoom
                    width={width}
                    height={height}
                    scaleXMin={1 / 8}
                    scaleXMax={2}
                    scaleYMin={1 / 8}
                    scaleYMax={2}
                    transformMatrix={initialTransform}
                >
                    {(zoom) => (
                        <svg>
                            <LineageTree
                                upstreamData={upstreamData}
                                downstreamData={downstreamData}
                                zoom={zoom}
                                onEntityClick={jest.fn()}
                                onLineageExpand={jest.fn()}
                                canvasHeight={yMax}
                                margin={margin}
                                direction={Direction.Upstream}
                                setIsDraggingNode={jest.fn()}
                                draggedNodes={{}}
                                setDraggedNodes={jest.fn()}
                                onEntityCenter={jest.fn()}
                                setHoveredEntity={jest.fn()}
                                fetchedEntities={mockFetchedEntities}
                            />
                        </svg>
                    )}
                </Zoom>
            </TestPageContainer>,
        );

        expect(getByTestId('edge-urn:li:dataset:6-urn:li:dataset:5-Upstream')).toBeInTheDocument();
        expect(getByTestId('edge-urn:li:dataset:4-urn:li:dataset:6-Upstream')).toBeInTheDocument();
        expect(getByTestId('edge-urn:li:dataset:4-urn:li:dataset:5-Upstream')).toBeInTheDocument();
        expect(getByTestId('edge-urn:li:dataset:3-urn:li:dataset:4-Upstream')).toBeInTheDocument();

        expect(getByTestId('node-urn:li:dataset:6-Upstream')).toBeInTheDocument();
        expect(getByTestId('node-urn:li:dataset:5-Upstream')).toBeInTheDocument();
        expect(getByTestId('node-urn:li:dataset:4-Upstream')).toBeInTheDocument();
        expect(getByTestId('node-urn:li:dataset:3-Upstream')).toBeInTheDocument();
    });
});
