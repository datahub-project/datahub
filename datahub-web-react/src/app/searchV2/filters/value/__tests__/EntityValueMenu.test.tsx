import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { FieldType, FilterValueOption } from '@app/searchV2/filters/types';
import EntityValueMenu from '@app/searchV2/filters/value/EntityValueMenu';
import { CONTAINER_FILTER_NAME } from '@app/searchV2/utils/constants';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { Container, Entity, EntityType } from '@types';

const container = (urn: string, name: string, parents: Entity[] = []): Container =>
    ({
        urn,
        type: EntityType.Container,
        properties: { name },
        parentContainers: { count: parents.length, containers: parents },
    }) as unknown as Container;

const petshopEast = container('urn:li:container:petshop_east', 'petshop_east');
const petshopWest = container('urn:li:container:petshop_west', 'petshop_west');

const LONG_ARCHIVE = 'petshop_loyalty_programme_archive';
const archive2019 = container('urn:li:container:archive_2019', `${LONG_ARCHIVE}_2019_to_2024`);
const archive2014 = container('urn:li:container:archive_2014', `${LONG_ARCHIVE}_2014_to_2018`);

const options: FilterValueOption[] = [
    container('urn:li:container:east_store', 'store', [petshopEast]),
    container('urn:li:container:west_store', 'store', [petshopWest]),
    container('urn:li:container:inventory', 'inventory', [petshopEast]),
    container('urn:li:container:orders_2019', 'orders', [archive2019]),
    container('urn:li:container:orders_2014', 'orders', [archive2014]),
].map((entity) => ({ value: entity.urn, entity, count: 1 }));

describe('EntityValueMenu', () => {
    it('labels same-named container options with the parent that tells them apart', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <EntityValueMenu
                        field={{
                            field: CONTAINER_FILTER_NAME,
                            displayName: 'Container',
                            type: FieldType.ENTITY,
                            entityTypes: [EntityType.Container],
                        }}
                        values={[]}
                        defaultOptions={options}
                        onChangeValues={vi.fn()}
                        onApply={vi.fn()}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        const paths = screen.getAllByTestId('filter-option-container-path').map((path) => path.textContent);
        expect(paths).toEqual(
            expect.arrayContaining([
                'petshop_east.store',
                'petshop_west.store',
                'petsho\u2026to_2024.orders',
                'petsho\u2026to_2018.orders',
            ]),
        );
        expect(paths).toHaveLength(4);
        expect(screen.getByText('inventory')).toBeInTheDocument();
        expect(screen.getAllByTestId('filter-option-container-path-elided')).toHaveLength(2);
    });
});
