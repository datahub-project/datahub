import {
    dataJob1,
    dataset3,
    dataset3WithLineage,
    dataset4,
    dataset4WithLineage,
    dataset5,
    dataset5WithLineage,
    dataset6WithLineage,
    dataFlow1,
} from '../../../Mocks';
import { DataPlatform, EntityType, RelationshipDirection } from '../../../types.generated';
import { getTestEntityRegistry } from '../../../utils/test-utils/TestPageContainer';
import { Direction, FetchedEntities } from '../types';
import constructTree from '../utils/constructTree';
import extendAsyncEntities from '../utils/extendAsyncEntities';

const testEntityRegistry = getTestEntityRegistry();
const kafkaPlatform: DataPlatform = dataset3.platform;

const airflowPlatform: DataPlatform = dataFlow1.platform;

describe('constructTree', () => {
    it('handles nodes without any lineage', () => {
        const mockFetchedEntities = {};
        expect(
            constructTree(
                { entity: dataset3, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
            ),
        ).toEqual({
            name: 'Yet Another Dataset',
            expandedName: 'Yet Another Dataset',
            urn: 'urn:li:dataset:3',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            children: [],
            icon: undefined,
            platform: kafkaPlatform,
            schemaMetadata: dataset3.schemaMetadata,
        });
    });

    it('handles nodes with downstream lineage', () => {
        const fetchedEntities = [
            { entity: dataset4, direction: Direction.Upstream, fullyFetched: false },
            { entity: dataset5, direction: Direction.Upstream, fullyFetched: false },
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

        expect(
            constructTree(
                { entity: dataset6WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Downstream,
                testEntityRegistry,
            ),
        ).toMatchObject({
            name: 'Display Name of Sixth',
            expandedName: 'Fully Qualified Name of Sixth Test Dataset',
            urn: 'urn:li:dataset:6',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            icon: undefined,
            platform: kafkaPlatform,
            schemaMetadata: dataset6WithLineage.schemaMetadata,
            children: [
                {
                    name: 'Fourth Test Dataset',
                    expandedName: 'Fourth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 0,
                    urn: 'urn:li:dataset:4',
                    countercurrentChildrenUrns: [],
                    children: [],
                    icon: undefined,
                    platform: kafkaPlatform,
                    status: null,
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

        expect(
            constructTree(
                { entity: dataset6WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
            ),
        ).toMatchObject({
            name: 'Display Name of Sixth',
            expandedName: 'Fully Qualified Name of Sixth Test Dataset',
            urn: 'urn:li:dataset:6',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            icon: undefined,
            platform: kafkaPlatform,
            schemaMetadata: dataset6WithLineage.schemaMetadata,
            children: [
                {
                    countercurrentChildrenUrns: [],
                    name: 'Fifth Test Dataset',
                    expandedName: 'Fifth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 0,
                    urn: 'urn:li:dataset:5',
                    children: [],
                    icon: undefined,
                    platform: kafkaPlatform,
                    status: null,
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

        expect(
            constructTree(
                { entity: dataset3WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
            ),
        ).toMatchObject({
            name: 'Yet Another Dataset',
            expandedName: 'Yet Another Dataset',
            urn: 'urn:li:dataset:3',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            icon: undefined,
            platform: kafkaPlatform,
            schemaMetadata: dataset3WithLineage.schemaMetadata,
            children: [
                {
                    name: 'Fourth Test Dataset',
                    expandedName: 'Fourth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 0,
                    urn: 'urn:li:dataset:4',
                    countercurrentChildrenUrns: ['urn:li:dataset:3'],
                    icon: undefined,
                    platform: kafkaPlatform,
                    status: null,
                    children: [
                        {
                            name: 'Display Name of Sixth',
                            expandedName: 'Fully Qualified Name of Sixth Test Dataset',
                            type: 'DATASET',
                            unexploredChildren: 0,
                            urn: 'urn:li:dataset:6',
                            countercurrentChildrenUrns: ['urn:li:dataset:4'],
                            icon: undefined,
                            platform: kafkaPlatform,
                            status: null,
                            children: [
                                {
                                    name: 'Fifth Test Dataset',
                                    expandedName: 'Fifth Test Dataset',
                                    type: EntityType.Dataset,
                                    unexploredChildren: 0,
                                    urn: 'urn:li:dataset:5',
                                    children: [],
                                    countercurrentChildrenUrns: [
                                        'urn:li:dataset:7',
                                        'urn:li:dataset:6',
                                        'urn:li:dataset:4',
                                    ],
                                    icon: undefined,
                                    platform: kafkaPlatform,
                                    status: null,
                                },
                            ],
                        },
                        {
                            name: 'Fifth Test Dataset',
                            expandedName: 'Fifth Test Dataset',
                            type: EntityType.Dataset,
                            unexploredChildren: 0,
                            urn: 'urn:li:dataset:5',
                            children: [],
                            countercurrentChildrenUrns: ['urn:li:dataset:7', 'urn:li:dataset:6', 'urn:li:dataset:4'],
                            icon: undefined,
                            platform: kafkaPlatform,
                            status: null,
                        },
                    ],
                },
            ],
        });
    });

    it('for a set of identical nodes, both will be referentially identical', () => {
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

        const tree = constructTree(
            { entity: dataset3WithLineage, type: EntityType.Dataset },
            mockFetchedEntities,
            Direction.Upstream,
            testEntityRegistry,
        );

        const fifthDatasetIntance1 = tree?.children?.[0]?.children?.[1];
        const fifthDatasetIntance2 = tree?.children?.[0]?.children?.[0]?.children?.[0];

        expect(fifthDatasetIntance1?.name).toEqual('Fifth Test Dataset');
        expect(fifthDatasetIntance2?.name).toEqual('Fifth Test Dataset');
        expect(fifthDatasetIntance1 === fifthDatasetIntance2).toEqual(true);
    });

    it('handles partially fetched graph with layers of lineage', () => {
        const fetchedEntities = [{ entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: false }];
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
        expect(
            constructTree(
                { entity: dataset3WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
            ),
        ).toMatchObject({
            name: 'Yet Another Dataset',
            expandedName: 'Yet Another Dataset',
            urn: 'urn:li:dataset:3',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            icon: undefined,
            platform: kafkaPlatform,
            schemaMetadata: dataset3WithLineage.schemaMetadata,
            children: [
                {
                    name: 'Fourth Test Dataset',
                    expandedName: 'Fourth Test Dataset',
                    type: EntityType.Dataset,
                    unexploredChildren: 2,
                    urn: 'urn:li:dataset:4',
                    children: [],
                    countercurrentChildrenUrns: ['urn:li:dataset:3'],
                    icon: undefined,
                    platform: kafkaPlatform,
                    status: null,
                },
            ],
        });
    });

    it('should not include a Dataset as a child if that Dataset has a Datajob child which points to the parent', () => {
        // dataset6 is downstream of dataset5 and datajob1, datajob 1 is downstream of dataset 5
        const updatedDataset6WithLineage = {
            ...dataset6WithLineage,
            downstream: null,
            upstream: {
                start: 0,
                count: 2,
                total: 2,
                relationships: [
                    {
                        type: 'DownstreamOf',
                        direction: RelationshipDirection.Incoming,
                        entity: dataset5,
                    },
                    {
                        type: 'DownstreamOf',
                        direction: RelationshipDirection.Incoming,
                        entity: dataJob1,
                    },
                ],
            },
        };
        const updatedDataset5WithLineage = {
            ...dataset5WithLineage,
            downstream: {
                ...dataset5WithLineage.downstream,
                relationships: [
                    ...dataset5WithLineage.downstream.relationships,
                    {
                        type: 'DownstreamOf',
                        direction: RelationshipDirection.Outgoing,
                        entity: dataJob1,
                    },
                ],
            },
        };
        const fetchedEntities = [
            { entity: updatedDataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataJob1, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity, type: entry.entity.type },
                    entry.fullyFetched,
                ),
            {} as FetchedEntities,
        );
        expect(
            constructTree(
                { entity: updatedDataset6WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
            ),
        ).toEqual({
            name: 'Display Name of Sixth',
            expandedName: 'Fully Qualified Name of Sixth Test Dataset',
            urn: 'urn:li:dataset:6',
            type: EntityType.Dataset,
            unexploredChildren: 0,
            icon: undefined,
            platform: kafkaPlatform,
            subtype: undefined,
            schemaMetadata: updatedDataset6WithLineage.schemaMetadata,
            children: [
                {
                    name: 'DataJobInfoName',
                    expandedName: 'DataFlowInfoName.DataJobInfoName',
                    type: EntityType.DataJob,
                    unexploredChildren: 0,
                    urn: dataJob1.urn,
                    children: [],
                    countercurrentChildrenUrns: [],
                    icon: '',
                    status: null,
                    platform: airflowPlatform,
                    subtype: undefined,
                },
            ],
        });
    });
});
