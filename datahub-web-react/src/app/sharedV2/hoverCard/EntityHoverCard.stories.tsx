import { MockedProvider } from '@apollo/client/testing';
import { Popover, Text } from '@components';
import { Meta, StoryObj } from '@storybook/react';
import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';
import styled from 'styled-components';

import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';
import EntityHoverCard from '@app/sharedV2/hoverCard/EntityHoverCard';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { EntityRegistryContext } from '@src/entityRegistryContext';

import {
    Container,
    CorpGroup,
    CorpUser,
    DataProduct,
    Dataset,
    Domain,
    Entity,
    EntityType,
    FabricType,
    GlossaryNode,
    GlossaryTerm,
    HealthStatus,
    HealthStatusType,
    OwnershipType,
    PropertyCardinality,
    StdDataType,
    Tag,
} from '@types';

import lookerLogo from '@images/lookerlogo.png';
import snowflakeLogo from '@images/snowflakelogo.png';

// ---------------------------------------------------------------------------------------------
// Fixtures. Partial GraphQL objects cast to their entity type — the card only reads the fields
// the hover fragments select, so this is what it sees in the app too.
// ---------------------------------------------------------------------------------------------

const DAY_MS = 24 * 60 * 60 * 1000;
const NOW = Date.now();

const janeDoe = {
    __typename: 'CorpUser',
    urn: 'urn:li:corpuser:jdoe',
    type: EntityType.CorpUser,
    username: 'jdoe',
    info: { displayName: 'Jane Doe', title: 'Analytics Engineer', email: 'jdoe@example.com', active: true },
    properties: { displayName: 'Jane Doe', title: 'Analytics Engineer', email: 'jdoe@example.com', active: true },
    editableProperties: {
        pictureLink: 'https://i.pravatar.cc/80?img=47',
        title: 'Analytics Engineer',
    },
} as CorpUser;

const samLee = {
    __typename: 'CorpUser',
    urn: 'urn:li:corpuser:slee',
    type: EntityType.CorpUser,
    username: 'slee',
    info: { displayName: 'Sam Lee', title: 'Data Steward', active: true },
    properties: { displayName: 'Sam Lee', title: 'Data Steward', active: true },
    editableProperties: { pictureLink: null },
} as CorpUser;

const dataPlatformTeam = {
    __typename: 'CorpGroup',
    urn: 'urn:li:corpGroup:data-platform',
    type: EntityType.CorpGroup,
    name: 'data-platform',
    info: { displayName: 'Data Platform' },
    properties: { displayName: 'Data Platform' },
} as CorpGroup;

const tagPii = {
    __typename: 'Tag',
    urn: 'urn:li:tag:PII',
    type: EntityType.Tag,
    name: 'PII',
    description: 'Contains personally identifiable information. Access is audited.',
    properties: { name: 'PII', description: 'Contains personally identifiable information.', colorHex: '#D9534F' },
    ownership: {
        owners: [
            { owner: samLee, type: OwnershipType.DataSteward, associatedUrn: 'urn:li:tag:PII' },
            { owner: dataPlatformTeam, type: OwnershipType.Dataowner, associatedUrn: 'urn:li:tag:PII' },
        ],
    },
} as Tag;

const tagCertified = {
    __typename: 'Tag',
    urn: 'urn:li:tag:Certified',
    type: EntityType.Tag,
    name: 'Certified',
    properties: { name: 'Certified', colorHex: '#2F855A' },
} as Tag;

const salesNode = {
    __typename: 'GlossaryNode',
    urn: 'urn:li:glossaryNode:sales',
    type: EntityType.GlossaryNode,
    properties: { name: 'Sales' },
} as GlossaryNode;

const termCustomerId = {
    __typename: 'GlossaryTerm',
    urn: 'urn:li:glossaryTerm:sales.customer-id',
    type: EntityType.GlossaryTerm,
    name: 'customer-id',
    hierarchicalName: 'Sales.Customer ID',
    properties: {
        name: 'Customer ID',
        description:
            'The stable identifier for a customer across all sales systems. Prefer this over email, which can change.',
        definition:
            'The stable identifier for a customer across all sales systems. Prefer this over email, which can change.',
        termSource: 'INTERNAL',
    },
    parentNodes: { count: 1, nodes: [salesNode] },
    ownership: {
        owners: [
            { owner: janeDoe, type: OwnershipType.Dataowner, associatedUrn: 'urn:li:glossaryTerm:sales.customer-id' },
        ],
    },
} as GlossaryTerm;

const commerceDomain = {
    __typename: 'Domain',
    urn: 'urn:li:domain:commerce',
    type: EntityType.Domain,
    id: 'commerce',
    properties: { name: 'Commerce' },
} as Domain;

const salesDomain = {
    __typename: 'Domain',
    urn: 'urn:li:domain:sales',
    type: EntityType.Domain,
    id: 'sales',
    properties: {
        name: 'Sales',
        description: 'Everything that turns a lead into revenue: pipeline, orders, invoicing and returns.',
    },
    parentDomains: { count: 1, domains: [commerceDomain] },
    ownership: {
        owners: [{ owner: dataPlatformTeam, type: OwnershipType.Dataowner, associatedUrn: 'urn:li:domain:sales' }],
    },
} as unknown as Domain;

const customer360 = {
    __typename: 'DataProduct',
    urn: 'urn:li:dataProduct:customer-360',
    type: EntityType.DataProduct,
    properties: { name: 'Customer 360' },
} as DataProduct;

const analyticsDb = {
    __typename: 'Container',
    urn: 'urn:li:container:analytics_db',
    type: EntityType.Container,
    properties: { name: 'analytics_db' },
    subTypes: { typeNames: ['Database'] },
} as Container;

const publicSchema = {
    __typename: 'Container',
    urn: 'urn:li:container:analytics_db.public',
    type: EntityType.Container,
    properties: { name: 'public' },
    subTypes: { typeNames: ['Schema'] },
} as Container;

const snowflake = {
    __typename: 'DataPlatform',
    urn: 'urn:li:dataPlatform:snowflake',
    type: EntityType.DataPlatform,
    name: 'snowflake',
    properties: { type: 'RELATIONAL_DB', displayName: 'Snowflake', logoUrl: snowflakeLogo },
};

const looker = {
    __typename: 'DataPlatform',
    urn: 'urn:li:dataPlatform:looker',
    type: EntityType.DataPlatform,
    name: 'looker',
    properties: { type: 'OTHERS', displayName: 'Looker', logoUrl: lookerLogo },
};

/** A dataset with only the fields a bare hover fragment returns: name, platform, and a description. */
const ordersMinimal = {
    __typename: 'Dataset',
    urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.orders,PROD)',
    type: EntityType.Dataset,
    name: 'analytics_db.public.orders',
    origin: FabricType.Prod,
    platform: snowflake,
    properties: {
        name: 'orders',
        qualifiedName: 'analytics_db.public.orders',
        description: 'One row per customer order. Refreshed hourly from the order service.',
    },
    parentContainers: { count: 2, containers: [publicSchema, analyticsDb] },
    subTypes: { typeNames: ['Table'] },
} as Dataset;

/** The same dataset with everything the search result fragment adds. */
const ordersFull = {
    ...ordersMinimal,
    editableProperties: {
        description:
            'One row per customer order, including cancelled and refunded orders. Refreshed hourly from the order service. Join to `customers` on `customer_id`; amounts are in the order currency, see `currency_code`.',
    },
    ownership: {
        owners: [
            { owner: janeDoe, type: OwnershipType.TechnicalOwner, associatedUrn: ordersMinimal.urn },
            { owner: dataPlatformTeam, type: OwnershipType.Dataowner, associatedUrn: ordersMinimal.urn },
        ],
    },
    globalTags: {
        tags: [
            { tag: tagCertified, associatedUrn: ordersMinimal.urn },
            { tag: tagPii, associatedUrn: ordersMinimal.urn },
        ],
    },
    glossaryTerms: { terms: [{ term: termCustomerId, associatedUrn: ordersMinimal.urn }] },
    domain: { domain: salesDomain, associatedUrn: ordersMinimal.urn },
    dataProduct: { relationships: [{ type: 'DataProductContains', entity: customer360 }] },
    health: [
        {
            type: HealthStatusType.Assertions,
            status: HealthStatus.Pass,
            message: 'All 12 assertions passing',
            causes: [],
        },
    ],
    versionProperties: {
        version: { versionTag: 'v3' },
        isLatest: true,
        aliases: [],
        versionSet: { urn: 'urn:li:versionSet:orders', type: EntityType.VersionSet },
    },
    structuredProperties: {
        properties: [
            {
                associatedUrn: ordersMinimal.urn,
                structuredProperty: {
                    urn: 'urn:li:structuredProperty:io.example.tier',
                    type: EntityType.StructuredProperty,
                    exists: true,
                    definition: {
                        qualifiedName: 'io.example.tier',
                        displayName: 'Tier',
                        cardinality: PropertyCardinality.Single,
                        valueType: {
                            urn: 'urn:li:dataType:datahub.string',
                            type: EntityType.DataType,
                            info: { type: StdDataType.String, qualifiedName: 'datahub.string' },
                        },
                        entityTypes: [],
                        allowedValues: [
                            {
                                value: { __typename: 'StringValue', stringValue: 'Gold' },
                                description: 'Trusted, production-grade. Safe to build on.',
                            },
                        ],
                    },
                    settings: { showAsAssetBadge: true, isHidden: false },
                },
                values: [{ __typename: 'StringValue', stringValue: 'Gold' }],
            },
        ],
    },
    statsSummary: { queryCountLast30Days: 1240, uniqueUserCountLast30Days: 31 },
    lastOperation: [{ lastUpdatedTimestamp: NOW - 2 * 60 * 60 * 1000 }],
    upstream: { total: 4, filtered: 0, relationships: [] },
    downstream: { total: 11, filtered: 0, relationships: [] },
} as unknown as Dataset;

/**
 * DataHub Cloud's fragments add usage percentiles to `statsSummary`; those drive the popularity
 * bars. Open source never has them, so this is what the same card shows in Cloud.
 */
const ordersCloud = {
    ...ordersFull,
    statsSummary: {
        queryCountLast30Days: 1240,
        uniqueUserCountLast30Days: 31,
        queryCountPercentileLast30Days: 92,
        uniqueUserPercentileLast30Days: 85,
    },
} as unknown as Dataset;

/** What the card looks like when the asset is on its way out and its checks are failing. */
const ordersLegacy = {
    ...ordersFull,
    urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.orders_legacy,PROD)',
    name: 'analytics_db.public.orders_legacy',
    properties: {
        ...ordersMinimal.properties,
        name: 'orders_legacy',
        qualifiedName: 'analytics_db.public.orders_legacy',
    },
    editableProperties: { description: 'Superseded by `orders`. Kept for backfills only.' },
    globalTags: { tags: [{ tag: tagPii, associatedUrn: 'urn:li:dataset:orders_legacy' }] },
    glossaryTerms: null,
    deprecation: {
        deprecated: true,
        note: 'Use `orders` instead. This table stops refreshing at the end of the quarter.',
        decommissionTime: Math.floor((NOW + 45 * DAY_MS) / 1000),
        actor: janeDoe.urn,
        actorEntity: janeDoe,
        replacement: ordersMinimal,
    },
    health: [
        {
            type: HealthStatusType.Assertions,
            status: HealthStatus.Fail,
            message: '3 of 12 assertions failing',
            causes: ['Freshness SLA missed', 'Row count dropped by 40%', 'Null customer_id'],
        },
    ],
    versionProperties: {
        version: { versionTag: 'v2' },
        isLatest: false,
        aliases: [],
        versionSet: { urn: 'urn:li:versionSet:orders', type: EntityType.VersionSet },
    },
    statsSummary: { queryCountLast30Days: 18, uniqueUserCountLast30Days: 2 },
    lastOperation: [{ lastUpdatedTimestamp: NOW - 19 * DAY_MS }],
    upstream: { total: 2, filtered: 0, relationships: [] },
    downstream: { total: 0, filtered: 0, relationships: [] },
} as unknown as Dataset;

const revenueDashboard = {
    __typename: 'Dashboard',
    urn: 'urn:li:dashboard:(looker,revenue-overview)',
    type: EntityType.Dashboard,
    dashboardId: 'revenue-overview',
    tool: 'looker',
    platform: looker,
    properties: {
        name: 'Revenue Overview',
        description: 'Weekly revenue by region and channel. The number leadership looks at on Monday.',
        lastModified: { time: NOW - 3 * DAY_MS },
        lastRefreshed: NOW - 6 * 60 * 60 * 1000,
    },
    ownership: {
        owners: [{ owner: janeDoe, type: OwnershipType.BusinessOwner, associatedUrn: 'urn:li:dashboard:revenue' }],
    },
    globalTags: { tags: [{ tag: tagCertified, associatedUrn: 'urn:li:dashboard:revenue' }] },
    domain: { domain: salesDomain, associatedUrn: 'urn:li:dashboard:revenue' },
    upstream: { total: 6, filtered: 0, relationships: [] },
    downstream: { total: 0, filtered: 0, relationships: [] },
} as unknown as Entity;

const propagatedFromColumn: AttributionDetails = {
    attribution: {
        time: NOW - 5 * DAY_MS,
        actor: janeDoe,
        source: { urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.customers,PROD)' },
        sourceDetail: [
            { key: 'propagated', value: 'true' },
            {
                key: 'origin',
                value: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.customers,PROD)',
            },
            { key: 'via', value: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.customers,PROD)' },
        ],
    },
} as unknown as AttributionDetails;

// ---------------------------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------------------------

/**
 * The card is always shown inside the app's `Popover`. This reproduces the popover's own surface
 * (overlay background, 12px radius, 12px padding, `shadowMd`) so the story shows what a user sees,
 * without needing an anchor element to float against.
 */
const PopoverSurface = styled.div`
    display: inline-block;
    border-radius: 12px;
    padding: 12px;
    background: ${(props) => props.theme.colors.bgOverlay};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    color: ${(props) => props.theme.colors.text};
    font-family: Mulish;
    font-size: 14px;
    line-height: 20px;
`;

const Trigger = styled.span`
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    border: 1px dashed ${(props) => props.theme.colors.border};
    color: ${(props) => props.theme.colors.textSecondary};
    cursor: default;
`;

const Gallery = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: flex-start;
    gap: 40px;
`;

const Figure = styled.figure`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin: 0;
    max-width: 520px;
`;

const Caption = styled.figcaption`
    display: flex;
    flex-direction: column;
    gap: 2px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

function Labelled({ label, note, children }: { label: string; note?: string; children: React.ReactNode }) {
    return (
        <Figure>
            <Caption>
                <Text size="md" weight="semiBold" color="gray" colorLevel={800}>
                    {label}
                </Text>
                {note && <Text size="sm">{note}</Text>}
            </Caption>
            <PopoverSurface>{children}</PopoverSurface>
        </Figure>
    );
}

type CardArgs = {
    entity: Entity;
    propagationDetails?: AttributionDetails;
};

function CardOnSurface({ entity, propagationDetails }: CardArgs) {
    return (
        <PopoverSurface>
            <EntityHoverCard entity={entity} propagationDetails={propagationDetails} />
        </PopoverSurface>
    );
}

/** Wraps stories in the app contexts the card and its badges read: router, entity registry, Apollo. */
function AppProviders({ children }: { children: React.ReactNode }) {
    const entityRegistry = useMemo(() => buildEntityRegistryV2(), []);
    return (
        <MockedProvider mocks={[]} addTypename={false}>
            <MemoryRouter>
                <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>
            </MemoryRouter>
        </MockedProvider>
    );
}

// ---------------------------------------------------------------------------------------------
// Stories
// ---------------------------------------------------------------------------------------------

const meta = {
    title: 'Entity Hover Card / EntityHoverCard',
    component: EntityHoverCard,
    parameters: {
        layout: 'padded',
        docs: {
            subtitle:
                'The one card shown when hovering any entity: tag and term pills, owner avatars, ' +
                'lineage nodes, search-result links, and so on.',
            description: {
                component:
                    'Every section is data-gated, so the same component ranges from a name-plus-path chip ' +
                    '(a tag pill hover) up to the full asset summary shown here for a dataset. ' +
                    'Use the **Overview** story to compare what already existed with what this PR adds.',
            },
        },
    },
    decorators: [
        (Story) => (
            <AppProviders>
                <Story />
            </AppProviders>
        ),
    ],
    render: (args) => <CardOnSurface {...args} />,
} satisfies Meta<CardArgs>;

export default meta;
type Story = StoryObj<typeof meta>;

/**
 * Side-by-side of what a hover showed before this PR and what it shows after, on the same data.
 */
export const Overview: Story = {
    args: { entity: ordersFull },
    render: () => {
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 48 }}>
                <section>
                    <div style={{ marginBottom: 16 }}>
                        <Text size="lg" weight="bold">
                            Dataset
                        </Text>
                        <Text size="md" color="gray">
                            Left: what the old hover actually rendered (the search card without its pills). Right: the
                            new card on the same data.
                        </Text>
                    </div>
                    <Gallery>
                        <Labelled
                            label="Before — bare hover fragment"
                            note="Old cards were fed by whatever the trigger had loaded; a tag or owner pill hover only had name, platform and description."
                        >
                            <EntityHoverCard entity={ordersMinimal} />
                        </Labelled>
                        <Labelled
                            label="After — full result fragment"
                            note="Added in this PR: Owners, Tags, Terms, Domain, Data Product sections. Restored from the old card: header badges (health, tier, version) and the usage footer (queries, lineage, freshness)."
                        >
                            <EntityHoverCard entity={ordersFull} />
                        </Labelled>
                        <Labelled
                            label="After — deprecated + failing"
                            note="Deprecation and health badges sit in the header exactly where the search card puts them; the hover to explain them still works."
                        >
                            <EntityHoverCard entity={ordersLegacy} />
                        </Labelled>
                        <Labelled
                            label="After — DataHub Cloud"
                            note="Cloud adds usage percentiles to statsSummary, which is all the popularity bars need. Same component, no Cloud-specific code path."
                        >
                            <EntityHoverCard entity={ordersCloud} />
                        </Labelled>
                    </Gallery>
                </section>

                <section>
                    <div style={{ marginBottom: 16 }}>
                        <Text size="lg" weight="bold">
                            Other entity types
                        </Text>
                        <Text size="md" color="gray">
                            These previously each had their own hover card. They now share the same component and simply
                            show fewer sections.
                        </Text>
                    </div>
                    <Gallery>
                        <Labelled
                            label="Tag"
                            note="Hovering a tag pill. Header shrink-wraps when there is nothing below it."
                        >
                            <EntityHoverCard entity={tagCertified} />
                        </Labelled>
                        <Labelled label="Tag with description and owners">
                            <EntityHoverCard entity={tagPii} />
                        </Labelled>
                        <Labelled label="Glossary term" note="Path shows the parent node.">
                            <EntityHoverCard entity={termCustomerId} />
                        </Labelled>
                        <Labelled label="Domain" note="Path shows the parent domain.">
                            <EntityHoverCard entity={salesDomain} />
                        </Labelled>
                        <Labelled label="User (owner pill)">
                            <EntityHoverCard entity={janeDoe} />
                        </Labelled>
                        <Labelled label="Group">
                            <EntityHoverCard entity={dataPlatformTeam} />
                        </Labelled>
                        <Labelled
                            label="Dashboard"
                            note="Freshness reads lastRefreshed / lastModified for dashboards and charts."
                        >
                            <EntityHoverCard entity={revenueDashboard} />
                        </Labelled>
                        <Labelled
                            label="Propagated tag"
                            note="When the hovered association was propagated, the card says who added it, when, and where it came from."
                        >
                            <EntityHoverCard entity={tagPii} propagationDetails={propagatedFromColumn} />
                        </Labelled>
                    </Gallery>
                </section>
            </div>
        );
    },
};

export const DatasetMinimal: Story = {
    name: 'Dataset — minimal fragment',
    args: { entity: ordersMinimal },
};

export const DatasetFull: Story = {
    name: 'Dataset — full',
    args: { entity: ordersFull },
};

export const DatasetCloud: Story = {
    name: 'Dataset — DataHub Cloud (popularity)',
    args: { entity: ordersCloud },
};

export const DatasetDeprecated: Story = {
    name: 'Dataset — deprecated and unhealthy',
    args: { entity: ordersLegacy },
};

export const Dashboard: Story = {
    args: { entity: revenueDashboard },
};

export const TagPill: Story = {
    name: 'Tag',
    args: { entity: tagPii },
};

export const PropagatedTag: Story = {
    args: { entity: tagPii, propagationDetails: propagatedFromColumn },
};

export const Term: Story = {
    name: 'Glossary term',
    args: { entity: termCustomerId },
};

export const DomainCard: Story = {
    name: 'Domain',
    args: { entity: salesDomain },
};

export const User: Story = {
    args: { entity: janeDoe },
};

/** The real popover, pinned open, so the chrome and offset are the genuine ones. */
export const InPopover: Story = {
    name: 'In the real Popover',
    args: { entity: ordersFull },
    render: ({ entity, propagationDetails }) => (
        <div style={{ padding: '8px 0 480px' }}>
            <Popover
                open
                placement="bottomLeft"
                zIndex={1100}
                content={<EntityHoverCard entity={entity} propagationDetails={propagationDetails} />}
            >
                <Trigger>hover target</Trigger>
            </Popover>
        </div>
    ),
};
