import {
    dataset3,
    dataset3WithLineage,
    dataset4,
    dataset4WithLineage,
    dataset5,
    dataset5WithLineage,
    dataset6WithLineage,
} from '../../../Mocks';
import { EntityType } from '../../../types.generated';
import { Direction, FetchedEntities } from '../types';
import constructTree from '../utils/constructTree';
import extendAsyncEntities from '../utils/extendAsyncEntities';

describe('constructTree', () => {
    it('handles nodes without any lineage', () => {
        const mockFetchedEntities = {};
        expect(constructTree(dataset3, mockFetchedEntities, Direction.Upstream)).toEqual({
            name: 'Yet Another Dataset',
            urn: 'urn:li:dataset:3',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            children: [],
        });
    });

    it('handles nodes with downstream lineage', () => {
        const fetchedEntities = [
            { entity: dataset4, direction: Direction.Upstream, fullyFetched: false },
            { entity: dataset5, direction: Direction.Upstream, fullyFetched: false },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        expect(constructTree(dataset6WithLineage, mockFetchedEntities, Direction.Downstream)).toEqual({
            name: 'Sixth Test Dataset',
            urn: 'urn:li:dataset:6',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            children: [
                {
                    name: 'Fourth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 0,
                    urn: 'urn:li:dataset:4',
                    countercurrentChildrenUrns: [],
                    children: [],
                },
            ],
        });
    });

    it('handles nodes with upstream lineage', () => {
        const fetchedEntities = [
            { entity: dataset4, direction: Direction.Upstream, fullyFetched: false },
            { entity: dataset5, direction: Direction.Upstream, fullyFetched: false },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        expect(constructTree(dataset6WithLineage, mockFetchedEntities, Direction.Upstream)).toEqual({
            name: 'Sixth Test Dataset',
            urn: 'urn:li:dataset:6',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            children: [
                {
                    countercurrentChildrenUrns: [],
                    name: 'Fifth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 0,
                    urn: 'urn:li:dataset:5',
                    children: [],
                },
            ],
        });
    });

    it('handles nodes with layers of lineage', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        expect(constructTree(dataset3WithLineage, mockFetchedEntities, Direction.Upstream)).toEqual({
            name: 'Yet Another Dataset',
            urn: 'urn:li:dataset:3',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            children: [
                {
                    name: 'Fourth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 0,
                    urn: 'urn:li:dataset:4',
                    countercurrentChildrenUrns: ['urn:li:dataset:3'],
                    children: [
                        {
                            name: 'Sixth Test Dataset',
                            type: 'DATASET',
                            unexploredChildren: 0,
                            urn: 'urn:li:dataset:6',
                            countercurrentChildrenUrns: ['urn:li:dataset:4'],
                            children: [
                                {
                                    name: 'Fifth Test Dataset',
                                    type: EntityType.Dataset,
                                    unexploredChildren: 0,
                                    urn: 'urn:li:dataset:5',
                                    children: [],
                                    countercurrentChildrenUrns: [
                                        'urn:li:dataset:7',
                                        'urn:li:dataset:6',
                                        'urn:li:dataset:4',
                                    ],
                                },
                            ],
                        },
                        {
                            name: 'Fifth Test Dataset',
                            type: EntityType.Dataset,
                            unexploredChildren: 0,
                            urn: 'urn:li:dataset:5',
                            children: [],
                            countercurrentChildrenUrns: ['urn:li:dataset:7', 'urn:li:dataset:6', 'urn:li:dataset:4'],
                        },
                    ],
                },
            ],
        });
    });

    it('maintains referential equality between identical nodes', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        const tree = constructTree(dataset3WithLineage, mockFetchedEntities, Direction.Upstream);

        const fifthDatasetIntance1 = tree?.children?.[0]?.children?.[0]?.children?.[0];
        const fifthDatasetIntance2 = tree?.children?.[0]?.children?.[1];

        expect(fifthDatasetIntance1?.name).toEqual('Fifth Test Dataset');
        expect(fifthDatasetIntance2?.name).toEqual('Fifth Test Dataset');
        expect(fifthDatasetIntance1 === fifthDatasetIntance2).toEqual(true);
    });

    it('handles partially fetched graph with layers of lineage', () => {
        const fetchedEntities = [{ entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: false }];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        expect(constructTree(dataset3WithLineage, mockFetchedEntities, Direction.Upstream)).toEqual({
            name: 'Yet Another Dataset',
            urn: 'urn:li:dataset:3',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            children: [
                {
                    name: 'Fourth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 2,
                    urn: 'urn:li:dataset:4',
                    children: [],
                    countercurrentChildrenUrns: ['urn:li:dataset:3'],
                },
            ],
        });
    });
});
