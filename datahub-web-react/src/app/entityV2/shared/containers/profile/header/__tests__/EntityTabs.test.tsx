import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EntityTabs } from '@app/entityV2/shared/containers/profile/header/EntityTabs';
import { EntityTab } from '@app/entityV2/shared/types';

const routeToTab = vi.fn();

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ entityData: {}, loading: false }),
    useBaseEntity: () => ({}),
    useRouteToTab: () => routeToTab,
}));

// Render each tab as a button so the test checks how EntityTabs wires keys, selection and routing.
vi.mock('@components', () => ({
    Tabs: ({
        tabs,
        selectedTab,
        onChange,
    }: {
        tabs: { key: string; name: string }[];
        selectedTab?: string;
        onChange: (key: string) => void;
    }) => (
        <div>
            <span data-testid="selected-tab">{selectedTab}</span>
            {tabs.map((tab) => (
                <button key={tab.key} type="button" onClick={() => onChange(tab.key)}>
                    {tab.name}
                </button>
            ))}
        </div>
    ),
}));

// Display names as they appear with a non-English UI language; paths stay in English.
const tabs: EntityTab[] = [
    {
        name: 'Sammanfattning',
        path: 'Summary',
        component: () => null,
        display: { visible: () => true, enabled: () => true },
    },
    { name: 'Kolumner', path: 'Columns', component: () => null, display: { visible: () => true, enabled: () => true } },
];

describe('EntityTabs', () => {
    beforeEach(() => {
        routeToTab.mockClear();
    });

    it('selects the routed tab by path and routes clicks by path, not by the translated name', () => {
        render(<EntityTabs tabs={tabs} selectedTab={tabs[1]} />);

        expect(screen.getByTestId('selected-tab')).toHaveTextContent('Columns');

        fireEvent.click(screen.getByText('Sammanfattning'));
        expect(routeToTab).toHaveBeenCalledWith({ tabName: 'Summary' });
    });

    it('redirects to the first enabled tab by path when no tab is routed', () => {
        render(<EntityTabs tabs={tabs} />);

        expect(routeToTab).toHaveBeenCalledWith({ tabName: 'Summary', method: 'replace' });
    });
});
