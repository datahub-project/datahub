import React from 'react';
import { Tabs } from "@components";
import { Tab } from "@components/components/Tabs/Tabs";
import GlossaryTab from './GlossaryTab';

export default function FormTabs() {
    const tabs: Tab[] = [
        // {
        //     name: 'Domains',
        //     key: 'domains',
        //     component: <>Domains</>,

        // },
        {
            name: 'Glossary',
            key: 'glossary',
            component: <GlossaryTab />,
        },
    ]

    return <Tabs
        tabs={tabs}
    />
}