import { Avatar, Input, Pill } from '@components';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { ChartLine } from '@phosphor-icons/react/dist/csr/ChartLine';
import { ShareNetwork } from '@phosphor-icons/react/dist/csr/ShareNetwork';
import { Swap } from '@phosphor-icons/react/dist/csr/Swap';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { FilterBar } from '@components/components/FilterBar/FilterBar';
import { FilterPopover } from '@components/components/FilterBar/components';
import { FilterField, FilterGroup, FilterValueEditorProps } from '@components/components/FilterBar/types';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import TagPill from '@app/sharedV2/tags/TagPill';

import { Domain, EntityType } from '@types';

import DbtLogo from '@images/dbtlogo.png';
import KafkaLogo from '@images/kafkalogo.png';
import LookerLogo from '@images/lookerlogo.png';
import SnowflakeLogo from '@images/snowflakelogo.png';

const Frame = styled.div`
    width: min(960px, calc(100vw - 64px));
    min-height: 180px;
    padding: 24px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    background: ${(props) => props.theme.colors.bg};
`;

const Header = styled.div`
    margin-bottom: 18px;
`;

const Title = styled.div`
    margin-bottom: 4px;
    color: ${(props) => props.theme.colors.text};
    font-size: 18px;
    font-weight: 700;
`;

const Description = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 13px;
`;

const PlatformLogo = styled.img`
    width: 16px;
    height: 16px;
    border-radius: 2px;
    object-fit: contain;
`;

const TagColorDot = styled.span<{ $color: string }>`
    display: inline-block;
    width: 12px;
    height: 12px;
    border-radius: 100%;
    background: ${(props) => props.$color};
`;

const DEMO_TAGS = [
    { value: 'certified', label: 'Certified', color: '#248F5B' },
    { value: 'deprecated', label: 'Deprecated', color: '#D3382F' },
    { value: 'pii', label: 'PII', color: '#7C3AED' },
    { value: 'tier-1', label: 'Tier 1', color: '#2563EB' },
];

function mockDomain(name: string, colorHex: string): Domain {
    return {
        urn: `urn:li:domain:${name.toLowerCase()}`,
        type: EntityType.Domain,
        id: name.toLowerCase(),
        properties: { name },
        displayProperties: { colorHex },
    } as Domain;
}

function TextValueEditor({ rule, onChange, trigger }: FilterValueEditorProps) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            trigger={React.cloneElement(trigger, { onClick: () => setIsOpen(!isOpen) })}
        >
            <Input
                value={rule.values[0] ?? ''}
                setValue={(value) => onChange({ ...rule, values: value ? [value] : [] })}
                placeholder="Enter text"
            />
        </FilterPopover>
    );
}

const operators = {
    entity: [
        { value: 'is', label: 'is' },
        { value: 'is_not', label: 'is not' },
        { value: 'exists', label: 'exists', requiresValue: false },
        { value: 'not_exists', label: 'does not exist', requiresValue: false },
    ],
    text: [
        { value: 'contains', label: 'contains' },
        { value: 'not_contains', label: 'does not contain' },
        { value: 'is', label: 'is' },
        { value: 'is_not', label: 'is not' },
        { value: 'exists', label: 'exists', requiresValue: false },
    ],
};

const fields: FilterField[] = [
    {
        field: 'entityType',
        label: 'Type',
        group: 'Core',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        searchable: true,
        showSelectAll: true,
        values: [
            { value: 'dataset', label: 'Dataset', count: 201100, icon: <Table size={16} /> },
            { value: 'dashboard', label: 'Dashboard', count: 21300, icon: <ChartBar size={16} /> },
            { value: 'chart', label: 'Chart', count: 8400, icon: <ChartLine size={16} /> },
            { value: 'dataFlow', label: 'Data flow', count: 1900, icon: <ShareNetwork size={16} /> },
        ],
    },
    {
        field: 'platform',
        label: 'Platform',
        group: 'Core',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        searchable: true,
        showSelectAll: true,
        values: [
            {
                value: 'snowflake',
                label: 'Snowflake',
                description: 'Data warehouse',
                count: 21800,
                icon: <PlatformLogo src={SnowflakeLogo} alt="" />,
            },
            {
                value: 'looker',
                label: 'Looker',
                description: 'Business intelligence',
                count: 468,
                icon: <PlatformLogo src={LookerLogo} alt="" />,
            },
            {
                value: 'dbt',
                label: 'dbt',
                description: 'Transformation',
                count: 15900,
                icon: <PlatformLogo src={DbtLogo} alt="" />,
            },
            {
                value: 'kafka',
                label: 'Kafka',
                description: 'Streaming',
                count: 7200,
                icon: <PlatformLogo src={KafkaLogo} alt="" />,
            },
        ],
    },
    {
        field: 'domain',
        label: 'Domain',
        group: 'Governance',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        searchable: true,
        values: [
            {
                value: 'finance',
                label: 'Finance',
                icon: <DomainColoredIcon domain={mockDomain('Finance', '#5480EF')} size={20} fontSize={12} />,
            },
            {
                value: 'marketing',
                label: 'Marketing',
                icon: <DomainColoredIcon domain={mockDomain('Marketing', '#E57716')} size={20} fontSize={12} />,
            },
            {
                value: 'product',
                label: 'Product',
                icon: <DomainColoredIcon domain={mockDomain('Product', '#13B185')} size={20} fontSize={12} />,
            },
            {
                value: 'sales',
                label: 'Sales',
                icon: <DomainColoredIcon domain={mockDomain('Sales', '#9B59B6')} size={20} fontSize={12} />,
            },
        ],
    },
    {
        field: 'owner',
        label: 'Owner',
        group: 'Governance',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        searchable: true,
        values: [
            {
                value: 'analytics',
                label: 'Analytics',
                icon: <Avatar name="Analytics" size="sm" showInPill={false} />,
            },
            {
                value: 'data-platform',
                label: 'Data platform',
                icon: <Avatar name="Data platform" size="sm" showInPill={false} />,
            },
            {
                value: 'finance-data',
                label: 'Finance data',
                icon: <Avatar name="Finance data" size="sm" showInPill={false} />,
            },
        ],
    },
    {
        field: 'tag',
        label: 'Tag',
        group: 'Governance',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        searchable: true,
        values: DEMO_TAGS.map((tag) => ({
            value: tag.value,
            label: tag.label,
            icon: <TagColorDot $color={tag.color} />,
        })),
        renderValueOption: (option) => {
            const tag = DEMO_TAGS.find((candidate) => candidate.value === option.value);
            return <TagPill name={option.label} color={tag?.color} size="sm" />;
        },
    },
    {
        field: 'name',
        label: 'Name',
        group: 'Metadata',
        defaultOperator: 'contains',
        operators: operators.text,
        renderValueEditor: (props) => <TextValueEditor {...props} />,
    },
];

const executionFields: FilterField[] = [
    {
        field: 'source',
        label: 'Source',
        defaultOperator: 'is',
        operators: operators.entity,
        values: [
            {
                value: 'snowflake-prod',
                label: 'Snowflake production',
                icon: <PlatformLogo src={SnowflakeLogo} alt="" />,
            },
            { value: 'looker', label: 'Looker', icon: <PlatformLogo src={LookerLogo} alt="" /> },
            { value: 'dbt-cloud', label: 'dbt Cloud', icon: <PlatformLogo src={DbtLogo} alt="" /> },
        ],
    },
    {
        field: 'status',
        label: 'Result',
        defaultOperator: 'is',
        operators: operators.entity,
        values: [
            { value: 'succeeded', label: 'Succeeded' },
            { value: 'failed', label: 'Failed' },
            { value: 'running', label: 'Running' },
            { value: 'cancelled', label: 'Cancelled' },
        ],
    },
    {
        field: 'executor',
        label: 'Executor',
        defaultOperator: 'is',
        operators: operators.entity,
        values: [
            { value: 'remote', label: 'Remote executor' },
            { value: 'local', label: 'Local executor' },
        ],
    },
    {
        field: 'timeRange',
        label: 'Started',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'single',
        values: [
            { value: '24-hours', label: 'Last 24 hours' },
            { value: '7-days', label: 'Last 7 days' },
            { value: '30-days', label: 'Last 30 days' },
        ],
    },
];

const queryFields: FilterField[] = [
    {
        field: 'column',
        label: 'Column',
        defaultOperator: 'is',
        operators: operators.entity,
        values: [
            { value: 'customer_id', label: 'customer_id' },
            { value: 'order_id', label: 'order_id' },
            { value: 'created_at', label: 'created_at' },
        ],
    },
    {
        field: 'user',
        label: 'User',
        defaultOperator: 'is',
        operators: operators.entity,
        values: [
            {
                value: 'analytics',
                label: 'Analytics team',
                icon: <Avatar name="Analytics team" size="sm" showInPill={false} />,
            },
            {
                value: 'finance',
                label: 'Finance team',
                icon: <Avatar name="Finance team" size="sm" showInPill={false} />,
            },
            {
                value: 'data-platform',
                label: 'Data platform',
                icon: <Avatar name="Data platform" size="sm" showInPill={false} />,
            },
        ],
    },
    {
        field: 'queryText',
        label: 'SQL',
        defaultOperator: 'contains',
        operators: operators.text,
        renderValueEditor: (props) => <TextValueEditor {...props} />,
    },
];

type DemoProps = {
    initialValue: FilterGroup;
    title: string;
    description: string;
    allowGroups?: boolean;
    availableFields?: FilterField[];
};

function FilterBarDemo({ initialValue, title, description, allowGroups = false, availableFields = fields }: DemoProps) {
    const [value, setValue] = useState(initialValue);

    return (
        <Frame>
            <Header>
                <Title>{title}</Title>
                <Description>{description}</Description>
            </Header>
            <FilterBar value={value} fields={availableFields} onChange={setValue} allowGroups={allowGroups} />
        </Frame>
    );
}

const ALL_OWNER_OPTIONS = Array.from({ length: 250 }, (_, index) => ({
    value: `owner-${index + 1}`,
    label: `Data owner ${index + 1}`,
    description: index % 2 === 0 ? 'User' : 'Group',
    count: 250 - index,
    icon: <Avatar name={`Data owner ${index + 1}`} size="sm" showInPill={false} />,
}));

function LargeFacetDemo() {
    const [value, setValue] = useState<FilterGroup>({
        id: 'large-facet-root',
        match: 'all',
        filters: [
            {
                id: 'owner-filter',
                field: 'owner',
                operator: 'is',
                values: ['owner-198'],
            },
        ],
    });
    const [ownerQuery, setOwnerQuery] = useState('');
    const [ownerLimit, setOwnerLimit] = useState(25);
    const matchingOwners = ALL_OWNER_OPTIONS.filter((option) =>
        option.label.toLocaleLowerCase().includes(ownerQuery.toLocaleLowerCase()),
    );
    const ownerField: FilterField = {
        field: 'owner',
        label: 'Owner',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        searchable: true,
        loading: false,
        hasMore: ownerLimit < matchingOwners.length,
        values: matchingOwners.slice(0, ownerLimit),
        selectedOptions: ALL_OWNER_OPTIONS.filter((option) => value.filters[0]?.values.includes(option.value)),
        onSearch: (query) => {
            setOwnerQuery(query);
            setOwnerLimit(25);
        },
        onLoadMore: () => setOwnerLimit((currentLimit) => currentLimit + 25),
    };

    return (
        <Frame>
            <Header>
                <Title>Large server-backed facets</Title>
                <Description>
                    Search, counts, selected-value preservation, and paging on scroll — selections apply immediately, so
                    there is nothing to confirm.
                </Description>
            </Header>
            <FilterBar
                value={value}
                fields={fields.map((field) => (field.field === ownerField.field ? ownerField : field))}
                onChange={setValue}
            />
        </Frame>
    );
}

const meta = {
    title: 'Components / FilterBar',
    component: FilterBar,
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL, 'readyForDesignReview'],
        docs: {
            subtitle: 'A single progressively disclosed filter experience for simple and advanced filtering.',
        },
    },
} satisfies Meta<typeof FilterBar>;

export default meta;

type Story = StoryObj<typeof meta>;

export const searchResults: Story = {
    args: {
        value: { id: 'search-root', match: 'all', filters: [] },
        fields,
        onChange: () => {},
    },
    render: () => (
        <FilterBarDemo
            title="Search results"
            description="Click any operator or value. Add another field with Filter; all criteria match by default."
            initialValue={{
                id: 'search-root',
                match: 'all',
                filters: [
                    {
                        id: 'type-filter',
                        field: 'entityType',
                        operator: 'is',
                        values: ['dataset', 'dashboard'],
                    },
                    {
                        id: 'platform-filter',
                        field: 'platform',
                        operator: 'is',
                        values: ['snowflake'],
                    },
                ],
            }}
        />
    ),
};

export const largeServerBackedFacets: Story = {
    args: {
        value: { id: 'large-facet-root', match: 'all', filters: [] },
        fields,
        onChange: () => {},
    },
    render: () => <LargeFacetDemo />,
};

export const advancedViewBuilder: Story = {
    args: {
        value: { id: 'advanced-root', match: 'all', filters: [] },
        fields,
        onChange: () => {},
        allowGroups: true,
    },
    render: () => (
        <FilterBarDemo
            title="View definition"
            description="The same chips expose exclusion and text operators. Views additionally allow nested groups."
            allowGroups
            initialValue={{
                id: 'advanced-root',
                match: 'all',
                filters: [
                    {
                        id: 'domain-filter',
                        field: 'domain',
                        operator: 'is',
                        values: ['finance'],
                    },
                    {
                        id: 'tag-filter',
                        field: 'tag',
                        operator: 'is_not',
                        values: ['deprecated'],
                    },
                ],
                groups: [
                    {
                        id: 'advanced-group',
                        match: 'any',
                        filters: [
                            {
                                id: 'type-filter',
                                field: 'entityType',
                                operator: 'is',
                                values: ['dataset'],
                            },
                            {
                                id: 'owner-filter',
                                field: 'owner',
                                operator: 'is',
                                values: ['analytics'],
                            },
                        ],
                    },
                ],
            }}
        />
    ),
};

export const browseSidebar: Story = {
    args: {
        value: { id: 'browse-root', match: 'all', filters: [] },
        fields,
        onChange: () => {},
    },
    render: () => (
        <FilterBarDemo
            title="Browse documents"
            description="A compact surface starts with pinned criteria and progressively reveals the full field catalog."
            initialValue={{
                id: 'browse-root',
                match: 'all',
                filters: [
                    {
                        id: 'domain-filter',
                        field: 'domain',
                        operator: 'is',
                        values: ['product'],
                    },
                ],
            }}
        />
    ),
};

export const homePageAssetModule: Story = {
    args: {
        value: { id: 'home-root', match: 'all', filters: [] },
        fields,
        onChange: () => {},
        allowGroups: true,
    },
    render: () => (
        <FilterBarDemo
            title="Home module · Finance assets"
            description="Module authors use the same language as search, with groups available for a saved dynamic collection."
            allowGroups
            initialValue={{
                id: 'home-root',
                match: 'all',
                filters: [
                    {
                        id: 'home-domain',
                        field: 'domain',
                        operator: 'is',
                        values: ['finance'],
                    },
                    {
                        id: 'home-tag',
                        field: 'tag',
                        operator: 'is_not',
                        values: ['deprecated'],
                    },
                ],
                groups: [
                    {
                        id: 'home-types',
                        match: 'any',
                        filters: [
                            {
                                id: 'home-type',
                                field: 'entityType',
                                operator: 'is',
                                values: ['dataset', 'dashboard'],
                            },
                            {
                                id: 'home-platform',
                                field: 'platform',
                                operator: 'is',
                                values: ['looker'],
                            },
                        ],
                    },
                ],
            }}
        />
    ),
};

export const ingestionExecutions: Story = {
    args: {
        value: { id: 'executions-root', match: 'all', filters: [] },
        fields: executionFields,
        onChange: () => {},
    },
    render: () => (
        <FilterBarDemo
            title="Ingestion executions"
            description="A list page pins its most common dimensions while keeping less common fields behind Filter."
            availableFields={executionFields}
            initialValue={{
                id: 'executions-root',
                match: 'all',
                filters: [
                    {
                        id: 'execution-source',
                        field: 'source',
                        operator: 'is',
                        values: ['snowflake-prod'],
                    },
                    {
                        id: 'execution-result',
                        field: 'status',
                        operator: 'is_not',
                        values: ['succeeded'],
                    },
                    {
                        id: 'execution-started',
                        field: 'timeRange',
                        operator: 'is',
                        values: ['7-days'],
                    },
                ],
            }}
        />
    ),
};

const LINEAGE_LENSES = [
    { value: 'none', label: 'None' },
    { value: 'platform', label: 'Platform' },
    { value: 'dataProduct', label: 'Data product' },
    { value: 'domain', label: 'Domain' },
    { value: 'glossaryTerm', label: 'Glossary term' },
    { value: 'structuredProperty', label: 'Structured property' },
];

const LINEAGE_TYPE_FIELD: FilterField = {
    field: 'entityType',
    label: 'Type',
    defaultOperator: 'is',
    operators: operators.entity,
    selectionMode: 'multiple',
    values: [
        { value: 'dataset', label: 'Dataset', icon: <Table size={16} /> },
        { value: 'dashboard', label: 'Dashboard', icon: <ChartBar size={16} /> },
        { value: 'chart', label: 'Chart', icon: <ChartLine size={16} /> },
        { value: 'dataJob', label: 'Data job', icon: <Swap size={16} /> },
        { value: 'transformation', label: 'Transformation', icon: <ShareNetwork size={16} /> },
    ],
};

/** Grouping constrains filtering: each lens contributes the field whose values are on the graph. */
const LENS_FILTER_FIELDS: Record<string, FilterField> = {
    platform: {
        field: 'platform',
        label: 'Platform',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            { value: 'snowflake', label: 'Snowflake', icon: <PlatformLogo src={SnowflakeLogo} alt="" /> },
            { value: 'dbt', label: 'dbt', icon: <PlatformLogo src={DbtLogo} alt="" /> },
            { value: 'looker', label: 'Looker', icon: <PlatformLogo src={LookerLogo} alt="" /> },
            { value: 'kafka', label: 'Kafka', icon: <PlatformLogo src={KafkaLogo} alt="" /> },
        ],
    },
    dataProduct: {
        field: 'dataProduct',
        label: 'Data product',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            { value: 'customer-360', label: 'Customer 360' },
            { value: 'revenue', label: 'Revenue reporting' },
        ],
    },
    domain: {
        field: 'domain',
        label: 'Domain',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            {
                value: 'finance',
                label: 'Finance',
                icon: <DomainColoredIcon domain={mockDomain('Finance', '#5480EF')} size={20} fontSize={12} />,
            },
            {
                value: 'marketing',
                label: 'Marketing',
                icon: <DomainColoredIcon domain={mockDomain('Marketing', '#E57716')} size={20} fontSize={12} />,
            },
            {
                value: 'product',
                label: 'Product',
                icon: <DomainColoredIcon domain={mockDomain('Product', '#13B185')} size={20} fontSize={12} />,
            },
            {
                value: 'github',
                label: 'GitHub',
                icon: <DomainColoredIcon domain={mockDomain('GitHub', '#9B59B6')} size={20} fontSize={12} />,
            },
        ],
    },
    glossaryTerm: {
        field: 'glossaryTerm',
        label: 'Glossary term',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            { value: 'pii', label: 'PII' },
            { value: 'revenue', label: 'Revenue' },
        ],
    },
    structuredProperty: {
        field: 'structuredProperty',
        label: 'Tier',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            { value: 'tier-1', label: 'Tier 1' },
            { value: 'tier-2', label: 'Tier 2' },
        ],
    },
};

const lineageViewingFields: FilterField[] = [
    {
        field: 'asOf',
        label: 'As of',
        defaultOperator: 'is',
        operators: [{ value: 'is', label: 'is' }],
        selectionMode: 'single',
        values: [
            { value: 'now', label: 'Now' },
            { value: '7-days', label: 'Last 7 days' },
            { value: '30-days', label: 'Last 30 days' },
            { value: 'aug-2024', label: 'Aug 18 – Aug 21, 2024' },
        ],
    },
    {
        field: 'source',
        label: 'Source',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            { value: 'automated', label: 'Automated' },
            { value: 'curated', label: 'Curated' },
            { value: 'manual', label: 'Manual' },
        ],
    },
    {
        field: 'environment',
        label: 'Environment',
        defaultOperator: 'is',
        operators: operators.entity,
        selectionMode: 'multiple',
        values: [
            { value: 'production', label: 'Production' },
            { value: 'staging', label: 'Staging' },
        ],
    },
];

const Panel = styled.div`
    display: flex;
    width: 380px;
    flex-direction: column;
    gap: 16px;
    padding: 16px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    background: ${(props) => props.theme.colors.bg};
`;

const PanelTitle = styled.div`
    color: ${(props) => props.theme.colors.text};
    font-size: 15px;
    font-weight: 700;
`;

const PanelSubtitle = styled.div`
    margin-top: 2px;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
`;

const PanelSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
`;

const SectionLabel = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 600;
`;

const LensRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

const PanelDivider = styled.div`
    height: 1px;
    background: ${(props) => props.theme.colors.border};
`;

function LineageGroupAndFilterPanel() {
    const [lens, setLens] = useState('platform');
    const [simplify, setSimplify] = useState<FilterGroup>({
        id: 'lineage-simplify',
        match: 'all',
        filters: [
            { id: 'lineage-type', field: 'entityType', operator: 'is_not', values: ['transformation'] },
            { id: 'lineage-lens-value', field: 'platform', operator: 'is_not', values: ['dbt'] },
        ],
    });
    const [viewing, setViewing] = useState<FilterGroup>({
        id: 'lineage-viewing',
        match: 'all',
        filters: [
            { id: 'lineage-source', field: 'source', operator: 'is', values: ['automated', 'curated'] },
            { id: 'lineage-environment', field: 'environment', operator: 'is', values: ['production'] },
        ],
    });

    const lensField = LENS_FILTER_FIELDS[lens];
    const simplifyFields = lensField ? [LINEAGE_TYPE_FIELD, lensField] : [LINEAGE_TYPE_FIELD];

    const changeLens = (nextLens: string) => {
        setLens(nextLens);
        // Filters belong to the active lens, so switching it drops the previous lens's values.
        setSimplify((current) => ({
            ...current,
            filters: current.filters.filter((filter) => filter.field === LINEAGE_TYPE_FIELD.field),
        }));
    };

    return (
        <Panel>
            <div>
                <PanelTitle>Group and filter</PanelTitle>
                <PanelSubtitle>Group assets and hide noise on the graph.</PanelSubtitle>
            </div>

            <PanelSection>
                <SectionLabel>Group by</SectionLabel>
                <LensRow>
                    {LINEAGE_LENSES.map((option) => (
                        <Pill
                            key={option.value}
                            label={option.label}
                            clickable
                            color={lens === option.value ? 'violet' : 'gray'}
                            onPillClick={() => changeLens(option.value)}
                        />
                    ))}
                </LensRow>
            </PanelSection>

            <PanelDivider />

            <PanelSection>
                <div>
                    <PanelTitle>Simplify my graph</PanelTitle>
                    <PanelSubtitle>Show or hide what&apos;s currently on the page.</PanelSubtitle>
                </div>
                <FilterBar
                    value={simplify}
                    fields={simplifyFields}
                    onChange={setSimplify}
                    labels={{ where: 'Show nodes where' }}
                />
            </PanelSection>

            <PanelDivider />

            <PanelSection>
                <div>
                    <PanelTitle>Change what I am viewing</PanelTitle>
                    <PanelSubtitle>
                        See lineage as it existed at a point in time, or by how it was created.
                    </PanelSubtitle>
                </div>
                <FilterBar
                    value={viewing}
                    fields={lineageViewingFields}
                    onChange={setViewing}
                    labels={{ where: 'Show lineage where' }}
                />
            </PanelSection>
        </Panel>
    );
}

export const lineageGroupAndFilter: Story = {
    args: {
        value: { id: 'lineage-simplify', match: 'all', filters: [] },
        fields: [LINEAGE_TYPE_FIELD],
        onChange: () => {},
    },
    parameters: {
        docs: {
            description: {
                story:
                    'The lineage "Group and filter" flyout. Group by stays a lens (it changes how nodes are drawn); ' +
                    'the two filter sections below use the shared chips, so include/exclude is the operator rather ' +
                    'than a separate mode. Switching the lens changes which filter fields are available.',
            },
        },
    },
    render: () => <LineageGroupAndFilterPanel />,
};

export const datasetQueries: Story = {
    args: {
        value: { id: 'queries-root', match: 'all', filters: [] },
        fields: queryFields,
        onChange: () => {},
    },
    render: () => (
        <FilterBarDemo
            title="Dataset · Queries tab"
            description="Entity tabs use domain-specific fields without inventing a different filter interaction."
            availableFields={queryFields}
            initialValue={{
                id: 'queries-root',
                match: 'all',
                filters: [
                    {
                        id: 'query-column',
                        field: 'column',
                        operator: 'is',
                        values: ['customer_id'],
                    },
                    {
                        id: 'query-text',
                        field: 'queryText',
                        operator: 'contains',
                        values: ['join'],
                    },
                ],
            }}
        />
    ),
};
