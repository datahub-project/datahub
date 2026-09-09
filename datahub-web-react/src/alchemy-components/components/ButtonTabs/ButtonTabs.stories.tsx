import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Rows } from '@phosphor-icons/react/dist/csr/Rows';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { Tab, TabButtonItem } from '@components/components/ButtonTabs/types';
import { Icon } from '@components/components/Icon';

import { ButtonTabs, TabButtons } from '.';

const TabLabel = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
`;

const IconOnly = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
`;

const meta = {
    title: 'Forms / ButtonTabs',
    component: ButtonTabs,
    parameters: {
        layout: 'padded',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle:
                'Segmented tab switch used across lineage, schema raw/table, home modules, and CodeBlock. Pass icons via the label ReactNode. Use fit="hug" for icon-only / compact; fit="fill" (default) to stretch equally.',
        },
    },
    args: {
        tabs: [
            { key: 'a', label: 'A', content: <div>A</div> },
            { key: 'b', label: 'B', content: <div>B</div> },
        ],
    },
} satisfies Meta<typeof ButtonTabs>;

export default meta;

type Story = StoryObj<typeof meta>;

const PANEL_TABS: Tab[] = [
    {
        key: 'explorer',
        label: (
            <TabLabel>
                <Icon icon={TreeStructure} size="lg" color="inherit" />
                Lineage
            </TabLabel>
        ),
        content: <div>Explorer panel</div>,
    },
    {
        key: 'impact',
        label: (
            <TabLabel>
                <Icon icon={Rows} size="lg" color="inherit" />
                Impact Analysis
            </TabLabel>
        ),
        content: <div>Impact panel</div>,
    },
];

export const WithPanels: Story = {
    name: 'With panels + icons (lineage-style)',
    args: {
        tabs: PANEL_TABS,
        defaultKey: 'explorer',
    },
};

export const ChromeOnly: Story = {
    name: 'TabButtons only (text)',
    args: {
        tabs: PANEL_TABS,
    },
    render: function Render() {
        const tabs: TabButtonItem[] = [
            { key: 'ansi', label: 'ANSI SQL' },
            { key: 'snowflake', label: 'Snowflake' },
        ];
        const [activeTab, setActiveTab] = useState('ansi');
        return <TabButtons tabs={tabs} activeTab={activeTab} onTabClick={setActiveTab} />;
    },
};

export const LineageTabs: Story = {
    name: 'TabButtons lineage (icon + text)',
    args: {
        tabs: PANEL_TABS,
    },
    render: function Render() {
        const tabs: TabButtonItem[] = [
            {
                key: 'explorer',
                label: (
                    <TabLabel>
                        <Icon icon={TreeStructure} size="lg" color="inherit" />
                        Lineage
                    </TabLabel>
                ),
                dataTestId: 'lineage-view-explorer',
            },
            {
                key: 'impact',
                label: (
                    <TabLabel>
                        <Icon icon={Rows} size="lg" color="inherit" />
                        Impact Analysis
                    </TabLabel>
                ),
                dataTestId: 'lineage-view-impact-analysis',
            },
        ];
        const [activeTab, setActiveTab] = useState('explorer');
        return <TabButtons tabs={tabs} activeTab={activeTab} onTabClick={setActiveTab} />;
    },
};

export const IconOnlyTabs: Story = {
    name: 'TabButtons icon-only (schema raw/table)',
    args: {
        tabs: PANEL_TABS,
    },
    render: function Render() {
        const tabs: TabButtonItem[] = [
            {
                key: 'tabular',
                label: (
                    <IconOnly>
                        <Icon icon={Table} size="lg" color="inherit" />
                    </IconOnly>
                ),
                dataTestId: 'schema-tabular-view-button',
            },
            {
                key: 'raw',
                label: (
                    <IconOnly>
                        <Icon icon={FileText} size="lg" color="inherit" />
                    </IconOnly>
                ),
                dataTestId: 'schema-raw-view-button',
            },
        ];
        const [activeTab, setActiveTab] = useState('tabular');
        return <TabButtons tabs={tabs} activeTab={activeTab} onTabClick={setActiveTab} fit="hug" />;
    },
};

export const TextHugged: Story = {
    name: 'TabButtons text hugged',
    args: {
        tabs: PANEL_TABS,
    },
    render: function Render() {
        const tabs: TabButtonItem[] = [
            { key: 'ansi', label: 'ANSI SQL' },
            { key: 'snowflake', label: 'Snowflake' },
        ];
        const [activeTab, setActiveTab] = useState('ansi');
        return <TabButtons tabs={tabs} activeTab={activeTab} onTabClick={setActiveTab} fit="hug" />;
    },
};
