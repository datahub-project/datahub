import { Pill } from '@components';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import styled from 'styled-components';

import { ResizablePills } from '@components/components/ResizablePills/index';

import BigQueryLogo from '@images/bigquerylogo.png';
import MySQLLogo from '@images/mysqllogo.png';
import PostgresLogo from '@images/postgreslogo.png';
import RedshiftLogo from '@images/redshiftlogo.png';
import SnowflakeLogo from '@images/snowflakelogo.png';

const meta: Meta<typeof ResizablePills> = {
    title: 'Components / ResizablePills',
    component: ResizablePills,
    parameters: {
        layout: 'padded',
    },
};

export default meta;

type Story = StoryObj<typeof ResizablePills>;

const PlatformIcon = styled.img`
    width: 16px;
    height: 16px;
    border-radius: 2px;
`;

const PlatformIconWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4px;
    background-color: #f5f5f5;
    border: 1px solid #d9d9d9;
    border-radius: 4px;
`;

const TooltipContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-width: 250px;
`;

const TooltipTitle = styled.div`
    font-weight: 600;
    margin-bottom: 4px;
`;

const ResizableContainer = styled.div<{ width: string }>`
    width: ${(props) => props.width};
    border: 2px dashed #ccc;
    padding: 16px;
    resize: horizontal;
    overflow: auto;
    min-width: 100px;
`;

// Mock data with local platform logos
const platforms = [
    { id: '1', name: 'Snowflake', icon: SnowflakeLogo },
    { id: '2', name: 'BigQuery', icon: BigQueryLogo },
    { id: '3', name: 'Redshift', icon: RedshiftLogo },
    { id: '4', name: 'Postgres', icon: PostgresLogo },
    { id: '5', name: 'MySQL', icon: MySQLLogo },
];

const groups = [
    { id: '1', name: 'Engineering' },
    { id: '2', name: 'Data Science' },
    { id: '3', name: 'Product Management' },
    { id: '4', name: 'Analytics' },
    { id: '5', name: 'Business Intelligence' },
];

export const IconPills: Story = {
    render: () => (
        <ResizableContainer width="400px">
            <h3>Platform Icons (Fixed Width)</h3>
            <ResizablePills
                items={platforms}
                getItemWidth={() => 28}
                renderPill={(platform) => (
                    <PlatformIconWrapper>
                        <PlatformIcon src={platform.icon} alt={platform.name} title={platform.name} />
                    </PlatformIconWrapper>
                )}
                overflowTooltipContent={(_hidden) => (
                    <TooltipContent>
                        <TooltipTitle>All Platforms</TooltipTitle>
                        {platforms.map((p) => (
                            <div key={p.id}>• {p.name}</div>
                        ))}
                    </TooltipContent>
                )}
                minContainerWidthForOne={60}
            />
            <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                Resize this container to see pills adapt
            </p>
        </ResizableContainer>
    ),
};

export const TextPills: Story = {
    render: () => (
        <ResizableContainer width="500px">
            <h3>Group Names (Dynamic Width)</h3>
            <ResizablePills
                items={groups}
                getItemWidth={(group) => group.name.length * 8 + 32}
                renderPill={(group) => <Pill variant="outline" label={group.name} size="sm" />}
                overflowTooltipContent={(hidden) => (
                    <TooltipContent>
                        <TooltipTitle>Hidden Groups</TooltipTitle>
                        {hidden.map((g) => (
                            <div key={g.id}>• {g.name}</div>
                        ))}
                    </TooltipContent>
                )}
                keyExtractor={(group) => group.id}
            />
            <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                Resize this container to see pills adapt
            </p>
        </ResizableContainer>
    ),
};

export const HybridPills: Story = {
    render: () => (
        <ResizableContainer width="600px">
            <h3>Platform Pills with Icons (Hybrid)</h3>
            <ResizablePills
                items={platforms}
                getItemWidth={(platform) => platform.name.length * 8 + 40}
                renderPill={(platform) => (
                    <Pill
                        variant="outline"
                        label={platform.name}
                        size="sm"
                        customIconRenderer={() => <PlatformIcon src={platform.icon} alt={platform.name} />}
                    />
                )}
                overflowTooltipContent={(hidden) => (
                    <TooltipContent>
                        <TooltipTitle>{hidden.length} More Platforms</TooltipTitle>
                        {hidden.map((p) => (
                            <div key={p.id} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <PlatformIcon src={p.icon} alt={p.name} />
                                {p.name}
                            </div>
                        ))}
                    </TooltipContent>
                )}
                keyExtractor={(platform) => platform.id}
            />
            <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                Resize this container to see pills adapt
            </p>
        </ResizableContainer>
    ),
};

export const WithoutTooltip: Story = {
    render: () => (
        <ResizableContainer width="400px">
            <h3>No Overflow Tooltip</h3>
            <ResizablePills
                items={groups}
                getItemWidth={(group) => group.name.length * 8 + 32}
                renderPill={(group) => <Pill variant="filled" label={group.name} size="sm" color="blue" />}
                keyExtractor={(group) => group.id}
            />
            <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                The +X button appears without a tooltip
            </p>
        </ResizableContainer>
    ),
};

export const CustomOverflowLabel: Story = {
    render: () => (
        <ResizableContainer width="400px">
            <h3>Custom Overflow Label</h3>
            <ResizablePills
                items={platforms}
                getItemWidth={(platform) => platform.name.length * 8 + 40}
                renderPill={(platform) => (
                    <Pill
                        variant="outline"
                        label={platform.name}
                        size="sm"
                        customIconRenderer={() => <PlatformIcon src={platform.icon} alt={platform.name} />}
                    />
                )}
                overflowLabel={(count) => `${count} more`}
                keyExtractor={(platform) => platform.id}
            />
            <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                Shows &quot;2 more&quot; instead of &quot;+2&quot;
            </p>
        </ResizableContainer>
    ),
};

export const LargeDataset: Story = {
    render: () => {
        const manyGroups = Array.from({ length: 50 }, (_, i) => ({
            id: String(i),
            name: `Group ${i + 1}`,
        }));

        return (
            <ResizableContainer width="600px">
                <h3>Performance Test (50 Items)</h3>
                <ResizablePills
                    items={manyGroups}
                    getItemWidth={(group) => group.name.length * 8 + 32}
                    renderPill={(group) => <Pill variant="outline" label={group.name} size="sm" />}
                    overflowTooltipContent={(hidden) => (
                        <TooltipContent>
                            <TooltipTitle>{hidden.length} Hidden Groups</TooltipTitle>
                            <div style={{ maxHeight: '200px', overflow: 'auto' }}>
                                {hidden.map((g) => (
                                    <div key={g.id}>• {g.name}</div>
                                ))}
                            </div>
                        </TooltipContent>
                    )}
                    keyExtractor={(group) => group.id}
                />
                <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                    Resize to test performance with many items
                </p>
            </ResizableContainer>
        );
    },
};

export const EmptyState: Story = {
    render: () => (
        <div style={{ padding: '16px', border: '2px dashed #ccc' }}>
            <h3>Empty Items Array</h3>
            <ResizablePills items={[]} getItemWidth={() => 50} renderPill={() => <div>Should not appear</div>} />
            <p style={{ marginTop: '16px', fontSize: '12px', color: '#666' }}>
                Component returns null when items is empty
            </p>
        </div>
    ),
};
