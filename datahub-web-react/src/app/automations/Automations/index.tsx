import React, { useState } from 'react';
import { uniq, orderBy } from 'lodash';

import { Button } from '@components';
import {
    AutomationsPageContainer,
    AutomationsSidebar,
    AutomationsContent,
    AutomationsContentHeader,
    AutomationsContentBody,
    AutomationsContentTabs,
    AutomationsContentTab,
    AutomationsBody,
} from './components';

import { env } from '../constants';

import { AutomationsContextProvider, useAutomationsContext } from './AutomationsProvider';

import { AutomationsListCard } from './ListCard';

import { AutomationContextProvider } from './AutomationProvider';
import { AutomationCreateModal } from './CreateModal';

import { EmptyState } from './EmptyState';

const AutomationPage = () => {
    // Rollout Variables (UI only)
    const { hideSidebar } = env;

    // Get list from context
    const { automations, isLoading } = useAutomationsContext();

    // Create Modal State
    const [isCreateOpen, setIsCreateOpen] = useState(false);

    // Get Categories
    const categories = uniq(
        automations.map((automation: any) => automation.details?.category).filter((category) => category !== ''),
    );

    // Build tabs
    const tabs: any = [
        {
            key: 'all',
            label: 'All',
            data: automations,
            count: automations.length,
        },
    ];
    categories.forEach((category: string) => {
        tabs.push({
            key: category,
            label: category,
            data: automations.filter((automation: any) => automation.details?.category === category),
            count: automations.filter((automation: any) => automation.details?.category === category).length,
        });
    });

    const [activeTab, setActiveTab] = useState(tabs[0].key);
    const data = tabs.filter((tab) => tab.key === activeTab)[0].data || [];

    return (
        <>
            <AutomationsPageContainer>
                {!hideSidebar && (
                    <AutomationsSidebar>
                        <h1>Sidebar</h1>
                    </AutomationsSidebar>
                )}
                <AutomationsContent>
                    <AutomationsContentHeader>
                        <div>
                            <h1>Automations</h1>
                            <p>Manage automated actions across your data assets</p>
                        </div>
                        <div>
                            <Button size="lg" icon="Add" onClick={() => setIsCreateOpen(!isCreateOpen)}>
                                Create
                            </Button>
                        </div>
                    </AutomationsContentHeader>
                    <AutomationsContentBody>
                        <AutomationsContentTabs>
                            {orderBy(tabs, ['count'], ['desc']).map((tab) => (
                                <AutomationsContentTab
                                    key={tab.key}
                                    isActive={activeTab === tab.key}
                                    onClick={() => setActiveTab(tab.key)}
                                >
                                    {tab.label}
                                    <span>{tab.count}</span>
                                </AutomationsContentTab>
                            ))}
                        </AutomationsContentTabs>
                        <AutomationsBody>
                            {data.map((item) => (
                                <AutomationsListCard key={item.key} automation={item} />
                            ))}
                        </AutomationsBody>
                        {!isLoading && data && data.length === 0 && <EmptyState />}
                    </AutomationsContentBody>
                </AutomationsContent>
            </AutomationsPageContainer>
            <AutomationContextProvider>
                <AutomationCreateModal isOpen={isCreateOpen} setIsOpen={setIsCreateOpen} />
            </AutomationContextProvider>
        </>
    );
};

// Export the Automations Page with the context provider
export const Automations = () => (
    <AutomationsContextProvider>
        <AutomationPage />
    </AutomationsContextProvider>
);
