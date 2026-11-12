import { MockedProvider } from '@apollo/client/testing';
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { History } from 'history';
import React from 'react';
import { useHistory } from 'react-router';
import { describe, expect, it, vi } from 'vitest';

import { IncidentListTable } from '@app/entityV2/shared/tabs/Incident/IncidentListTable';
import { IncidentListFilter, IncidentTable, IncidentTableRow } from '@app/entityV2/shared/tabs/Incident/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { IncidentPriority, IncidentStage, IncidentType } from '@types';

vi.mock('@src/app/entityV2/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: 'urn:li:dataset:test', entityData: undefined }),
}));
vi.mock('@app/entityV2/shared/tabs/Incident/IncidentResolveButton', () => ({ IncidentResolveButton: () => null }));
vi.mock('@app/entityV2/shared/tabs/Incident/IncidentAssigneeAvatarStack', () => ({
    IncidentAssigneeAvatarStack: () => null,
}));
vi.mock('@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer', () => ({
    IncidentDetailDrawer: () => null,
}));

const incident = (urn: string, title: string, priority: IncidentPriority) =>
    ({
        urn,
        title,
        priority,
        stage: IncidentStage.Triage,
        type: IncidentType.Operational,
        created: 1,
        assignees: [],
        linkedAssets: [],
    }) as unknown as IncidentTableRow;

const HIGH = incident('urn:li:incident:high', 'High incident', IncidentPriority.High);
const LOW = incident('urn:li:incident:low', 'Low incident', IncidentPriority.Low);

const incidentData = {
    incidents: [HIGH, LOW],
    groupBy: {
        priority: [
            { name: 'HIGH', icon: null, incidents: [HIGH], untransformedIncidents: [] },
            { name: 'LOW', icon: null, incidents: [LOW], untransformedIncidents: [] },
        ],
        stage: [],
        category: [],
        state: [],
    },
} as IncidentTable;

const filter = { groupBy: 'priority' } as IncidentListFilter;

let history: History | undefined;
const CaptureHistory = () => {
    history = useHistory();
    return null;
};

const isCollapsed = (group: string) =>
    !!within(screen.getByTestId(`incident-group-${group}`)).queryByRole('button', { name: 'Expand group' });

describe('IncidentListTable incident links', () => {
    it('opens the linked group for each new incident_urn, even after the first link', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer initialEntries={[`/?incident_urn=${HIGH.urn}`]}>
                    <CaptureHistory />
                    <IncidentListTable incidentData={incidentData} filter={filter} refetch={vi.fn()} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(isCollapsed('HIGH')).toBe(false);
        expect(isCollapsed('LOW')).toBe(true);

        act(() => history?.push(`/?incident_urn=${LOW.urn}`));

        expect(isCollapsed('LOW')).toBe(false);
        expect(isCollapsed('HIGH')).toBe(true);
    });

    it('reopens the linked group when the same link is followed again after the user closed it', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer initialEntries={[`/?incident_urn=${HIGH.urn}`]}>
                    <CaptureHistory />
                    <IncidentListTable incidentData={incidentData} filter={filter} refetch={vi.fn()} />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.click(
            within(screen.getByTestId('incident-group-HIGH')).getByRole('button', { name: 'Collapse group' }),
        );
        expect(isCollapsed('HIGH')).toBe(true);

        act(() => history?.push(`/?incident_urn=${HIGH.urn}`));

        expect(isCollapsed('HIGH')).toBe(false);
    });
});
