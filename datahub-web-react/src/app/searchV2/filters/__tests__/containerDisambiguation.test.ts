import {
    formatContainerPath,
    getContainerDisambiguationParents,
    getContainerPathSegments,
    getFullContainerPath,
} from '@app/searchV2/filters/containerDisambiguation';

import { Container, Entity, EntityType } from '@types';

const container = (urn: string, name: string, parents: Entity[] = []): Container =>
    ({
        urn,
        type: EntityType.Container,
        properties: { name },
        parentContainers: { count: parents.length, containers: parents },
    }) as unknown as Container;

const getLabel = (entity: Entity) => (entity as Container).properties?.name || entity.urn;

const option = (entity: Entity) => ({ value: entity.urn, entity });

const names = (parents: Entity[] | undefined) => parents?.map(getLabel);

const lakehouseA = container('urn:li:container:lakehouse_a', 'lakehouse_a');
const lakehouseB = container('urn:li:container:lakehouse_b', 'lakehouse_b');
const petshopEast = container('urn:li:container:petshop_east', 'petshop_east', [lakehouseA]);
const petshopWest = container('urn:li:container:petshop_west', 'petshop_west', [lakehouseA]);
const centralA = container('urn:li:container:central_a', 'petshop_central', [lakehouseA]);
const centralB = container('urn:li:container:central_b', 'petshop_central', [lakehouseB]);

describe('getContainerDisambiguationParents', () => {
    it('adds only the nearest parent when it tells same-named containers apart', () => {
        const eastStore = container('urn:li:container:east_store', 'store', [petshopEast, lakehouseA]);
        const westStore = container('urn:li:container:west_store', 'store', [petshopWest, lakehouseA]);

        const result = getContainerDisambiguationParents([option(eastStore), option(westStore)], getLabel);

        expect(names(result.get(eastStore.urn))).toEqual(['petshop_east']);
        expect(names(result.get(westStore.urn))).toEqual(['petshop_west']);
    });

    it('adds as many levels as it takes when the nearest parents share a name too', () => {
        const supplyA = container('urn:li:container:supply_a', 'supply', [centralA, lakehouseA]);
        const supplyB = container('urn:li:container:supply_b', 'Supply ', [centralB, lakehouseB]);

        const result = getContainerDisambiguationParents([option(supplyA), option(supplyB)], getLabel);

        expect(names(result.get(supplyA.urn))).toEqual(['petshop_central', 'lakehouse_a']);
        expect(names(result.get(supplyB.urn))).toEqual(['petshop_central', 'lakehouse_b']);
    });

    it('leaves unique names and non-container options alone', () => {
        const inventory = container('urn:li:container:inventory', 'inventory', [petshopEast]);
        const tag = { urn: 'urn:li:tag:store', type: EntityType.Tag } as Entity;
        const store = container('urn:li:container:east_store', 'store', [petshopEast]);

        const result = getContainerDisambiguationParents(
            [option(inventory), option(store), { value: tag.urn, entity: tag }, { value: 'no-entity' }],
            getLabel,
        );

        expect(result.size).toBe(0);
    });

    it('shows the whole chain when no level tells the options apart', () => {
        const storeOne = container('urn:li:container:store_1', 'store', [petshopEast, lakehouseA]);
        const storeTwo = container('urn:li:container:store_2', 'store', [petshopEast, lakehouseA]);
        const rootStore = container('urn:li:container:store_root', 'store');

        const result = getContainerDisambiguationParents(
            [option(storeOne), option(storeTwo), option(rootStore)],
            getLabel,
        );

        expect(names(result.get(storeOne.urn))).toEqual(['petshop_east', 'lakehouse_a']);
        expect(names(result.get(storeTwo.urn))).toEqual(['petshop_east', 'lakehouse_a']);
        expect(result.has(rootStore.urn)).toBe(false);
    });
});

describe('container paths', () => {
    it('formats parents outermost first, ending with the label', () => {
        expect(formatContainerPath('supply', [centralA, lakehouseA], getLabel)).toBe(
            'lakehouse_a.petshop_central.supply',
        );
    });

    it('builds the full path from all parent containers, or just the name without parents', () => {
        const supply = container('urn:li:container:supply_a', 'supply', [centralA, lakehouseA]);

        expect(getFullContainerPath(supply, getLabel)).toBe('lakehouse_a.petshop_central.supply');
        expect(
            getFullContainerPath({ urn: 'urn:li:container:bare', type: EntityType.Container } as Entity, getLabel),
        ).toBe('urn:li:container:bare');
    });
});

describe('getContainerPathSegments', () => {
    const chain = (...labels: string[]) => labels.map((label) => container(`urn:li:container:${label}`, label));

    it('shows a short path whole', () => {
        expect(getContainerPathSegments('store', chain('petshop_east', 'lakehouse_a'), getLabel)).toEqual([
            { key: 'urn:li:container:lakehouse_a', text: 'lakehouse_a' },
            { key: 'urn:li:container:petshop_east', text: 'petshop_east' },
            { key: 'self', text: 'store' },
        ]);
    });

    it('keeps the outermost parent and the name of a long path, collapsing the levels between', () => {
        const parents = chain('department', 'petshop_central', 'region', 'warehouse', 'lakehouse_alpha');

        expect(getContainerPathSegments('supply', parents, getLabel)).toEqual([
            { key: 'urn:li:container:lakehouse_alpha', text: 'lakeho\u2026e_alpha', hidden: 'lakehouse_alpha' },
            { key: 'collapsed', text: '\u2026', hidden: 'warehouse.region.petshop_central.department' },
            { key: 'self', text: 'supply' },
        ]);
    });

    it('shortens long names in the middle so names that differ at either end stay apart', () => {
        const archive = 'petshop_loyalty_programme_archive_2019_to_2024';

        expect(getContainerPathSegments('store', chain(archive), getLabel)).toEqual([
            { key: `urn:li:container:${archive}`, text: 'petsho\u2026to_2024', hidden: archive },
            { key: 'self', text: 'store' },
        ]);
    });
});
