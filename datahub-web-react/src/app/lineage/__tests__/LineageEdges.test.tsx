import { render } from '@testing-library/react';
import React from 'react';
import { dataset1, dataset4WithLineage } from '../../../Mocks';
import { getTestEntityRegistry } from '../../../utils/test-utils/TestPageContainer';
import LineageEdges from '../manage/LineageEdges';
import { Direction } from '../types';

const mockEntityRegistry = getTestEntityRegistry();
vi.mock('../../useEntityRegistry', () => ({
    useEntityRegistry: () => mockEntityRegistry,
}));

describe('LineageEdges', () => {
    it('should render an empty state if there are no lineage children or entities to add', () => {
        const { getByTestId, queryByTestId } = render(
            <LineageEdges
                entity={dataset1}
                lineageDirection={Direction.Upstream}
                entitiesToAdd={[]}
                entitiesToRemove={[]}
                setEntitiesToAdd={vi.fn}
                setEntitiesToRemove={vi.fn}
            />,
        );

        expect(getByTestId('empty-lineage')).toBeInTheDocument();
        expect(queryByTestId('lineage-entity-item')).not.toBeInTheDocument();
    });

    it('should render upstream children if the direction is upstream', async () => {
        const { queryByTestId, findAllByTestId, getByText } = render(
            <LineageEdges
                entity={dataset4WithLineage}
                lineageDirection={Direction.Upstream}
                entitiesToAdd={[]}
                entitiesToRemove={[]}
                setEntitiesToAdd={vi.fn}
                setEntitiesToRemove={vi.fn}
            />,
        );

        const entityItems = await findAllByTestId('lineage-entity-item');

        expect(queryByTestId('empty-lineage')).not.toBeInTheDocument();
        expect(entityItems).toHaveLength(2);
        expect(getByText(dataset4WithLineage.upstream.relationships[0].entity.properties.name)).toBeInTheDocument();
        expect(getByText(dataset4WithLineage.upstream.relationships[1].entity.properties.name)).toBeInTheDocument();
    });

    it('should render downstream children if the direction is downstream', async () => {
        const { queryByTestId, findAllByTestId, getByText } = render(
            <LineageEdges
                entity={dataset4WithLineage}
                lineageDirection={Direction.Downstream}
                entitiesToAdd={[]}
                entitiesToRemove={[]}
                setEntitiesToAdd={vi.fn}
                setEntitiesToRemove={vi.fn}
            />,
        );

        const entityItems = await findAllByTestId('lineage-entity-item');

        expect(queryByTestId('empty-lineage')).not.toBeInTheDocument();
        expect(entityItems).toHaveLength(1);
        expect(
            getByText(dataset4WithLineage.downstream.relationships[0]!.entity!.properties!.name!),
        ).toBeInTheDocument();
    });

    it('should remove entities from the displayed list if the urn is in entitiesToRemove', async () => {
        const { queryByTestId, findAllByTestId, getByText, queryByText } = render(
            <LineageEdges
                entity={dataset4WithLineage}
                lineageDirection={Direction.Upstream}
                entitiesToAdd={[]}
                entitiesToRemove={[dataset4WithLineage.upstream.relationships[1].entity]}
                setEntitiesToAdd={vi.fn}
                setEntitiesToRemove={vi.fn}
            />,
        );

        const entityItems = await findAllByTestId('lineage-entity-item');

        expect(queryByTestId('empty-lineage')).not.toBeInTheDocument();
        expect(entityItems).toHaveLength(1);
        expect(getByText(dataset4WithLineage.upstream.relationships[0].entity.properties.name)).toBeInTheDocument();
        expect(
            queryByText(dataset4WithLineage.upstream.relationships[1].entity.properties.name),
        ).not.toBeInTheDocument();
    });

    it('should append entities from entitiesToAdd to the displayed list', async () => {
        const { queryByTestId, findAllByTestId, getByText } = render(
            <LineageEdges
                entity={dataset4WithLineage}
                lineageDirection={Direction.Upstream}
                entitiesToAdd={[dataset1]}
                entitiesToRemove={[]}
                setEntitiesToAdd={vi.fn}
                setEntitiesToRemove={vi.fn}
            />,
        );

        const entityItems = await findAllByTestId('lineage-entity-item');

        expect(queryByTestId('empty-lineage')).not.toBeInTheDocument();
        expect(entityItems).toHaveLength(3);
        expect(getByText(dataset4WithLineage.upstream.relationships[0].entity.properties.name)).toBeInTheDocument();
        expect(getByText(dataset4WithLineage.upstream.relationships[1].entity.properties.name)).toBeInTheDocument();
        expect(getByText(dataset1.properties.name)).toBeInTheDocument();
    });
});
