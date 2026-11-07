import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { VerticalTabItem } from '@components/components/VerticalTabs/types';

import { VerticalTabs } from '.';

const ITEMS: VerticalTabItem[] = [
    { key: 'summary', label: 'Summary', icon: FileText },
    { key: 'columns', label: 'Columns', icon: ListBullets },
    { key: 'lineage', label: 'Lineage', icon: TreeStructure },
    { key: 'stats', label: 'Statistics', icon: ChartBar, disabled: true, tooltip: 'No statistics available' },
];

const Layout = styled.div`
    display: flex;
    height: 320px;
    border: 1px solid ${(props) => props.theme.colors.border};
`;

const Panel = styled.div`
    width: 240px;
    padding: 16px;
    color: ${(props) => props.theme.colors.text};
    border-right: 1px solid ${(props) => props.theme.colors.border};
`;

const meta = {
    title: 'Components / VerticalTabs',
    component: VerticalTabs,
    parameters: {
        layout: 'centered',
        badges: [BADGE.BETA],
        docs: {
            subtitle:
                'Icon rail of vertical tabs that only switches which panel the parent renders; it has no content panes of its own.',
        },
    },
    argTypes: {
        items: { description: 'Tabs to render: key, phosphor icon, optional label, tooltip, disabled.' },
        activeKey: { description: 'Key of the active tab; undefined renders no tab as active.' },
        onChange: { description: 'Called when a tab other than the active one is clicked.' },
        onTabClick: { description: 'Called on every click, including the active tab.' },
        position: {
            description: 'Side of the panel the rail sits on; sets the default tooltip placement.',
            control: 'radio',
            options: ['left', 'right'],
        },
    },
    args: {
        items: ITEMS,
        activeKey: 'summary',
        position: 'right',
    },
} satisfies Meta<typeof VerticalTabs>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Playground: Story = {
    render: function Render(args) {
        const [activeKey, setActiveKey] = useState(args.activeKey);
        return <VerticalTabs {...args} activeKey={activeKey} onChange={setActiveKey} />;
    },
};

export const IconOnly: Story = {
    name: 'Icon only (tooltips)',
    render: function Render(args) {
        const items = ITEMS.map(({ label, ...item }) => ({ ...item, tooltip: item.tooltip ?? label }));
        const [activeKey, setActiveKey] = useState(args.activeKey);
        return <VerticalTabs {...args} items={items} activeKey={activeKey} onChange={setActiveKey} />;
    },
};

export const CollapsiblePanel: Story = {
    name: 'Collapsible side panel',
    render: function Render() {
        const [activeKey, setActiveKey] = useState('summary');
        const [isClosed, setIsClosed] = useState(false);

        const items: VerticalTabItem[] = [
            {
                key: 'collapse',
                icon: isClosed ? ArrowLineLeft : ArrowLineRight,
                tooltip: isClosed ? 'Expand' : 'Collapse',
            },
            ...ITEMS,
        ];

        const handleTabClick = (key: string) => {
            if (key === 'collapse' || (key === activeKey && !isClosed)) {
                setIsClosed(!isClosed);
                return;
            }
            setActiveKey(key);
            setIsClosed(false);
        };

        return (
            <Layout>
                {!isClosed && <Panel>{ITEMS.find((item) => item.key === activeKey)?.label} panel</Panel>}
                <VerticalTabs
                    items={items}
                    activeKey={isClosed ? undefined : activeKey}
                    onTabClick={handleTabClick}
                    aria-label="Side panel"
                />
            </Layout>
        );
    },
};
