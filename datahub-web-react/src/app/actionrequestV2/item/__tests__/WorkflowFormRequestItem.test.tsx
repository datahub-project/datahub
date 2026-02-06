import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import WorkflowFormRequestItem from '@app/actionrequestV2/item/WorkflowFormRequestItem';
import { useListActionWorkflows } from '@app/workflows/hooks/useListActionWorkflows';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import {
    ActionRequest,
    ActionRequestOrigin,
    ActionRequestStatus,
    ActionRequestType,
    ActionWorkflowCategory,
    ActionWorkflowEntrypointType,
    EntityType,
} from '@types';

// Mock the hook that fetches workflows
vi.mock('@app/workflows/hooks/useListActionWorkflows', () => ({
    useListActionWorkflows: vi.fn(),
}));

const mockUseListActionWorkflows = vi.mocked(useListActionWorkflows);

describe('WorkflowFormRequestItem', () => {
    const workflowUrn = 'urn:li:actionWorkflow:test-workflow-123';

    const createActionRequest = (overrides?: Partial<ActionRequest>): ActionRequest =>
        ({
            urn: 'urn:li:actionRequest:123',
            type: ActionRequestType.WorkflowFormRequest,
            status: ActionRequestStatus.Pending,
            origin: ActionRequestOrigin.Manual,
            created: {
                time: 1641034800000,
                actor: {
                    urn: 'urn:li:corpuser:testuser',
                    username: 'testuser',
                    type: EntityType.CorpUser,
                },
            },
            entity: {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,test.dataset,PROD)',
                type: EntityType.Dataset,
                name: 'test_dataset',
            },
            params: {
                workflowFormRequest: {
                    workflowUrn,
                    category: ActionWorkflowCategory.Access,
                    fields: [],
                    stepState: {
                        stepId: 'review_step',
                    },
                },
            },
            ...overrides,
        }) as ActionRequest;

    const createWorkflowMock = (entrypointTypes: ActionWorkflowEntrypointType[]) => ({
        urn: workflowUrn,
        name: 'Dataset Access Request',
        description: 'Request access to sensitive datasets',
        category: ActionWorkflowCategory.Access,
        customCategory: null,
        trigger: {
            type: 'FORM_SUBMITTED',
            form: {
                fields: [],
                entityTypes: ['urn:li:entityType:datahub.dataset'],
                entrypoints: entrypointTypes.map((type) => ({
                    type,
                    label: type === ActionWorkflowEntrypointType.Home ? 'Home Label' : 'Request Access',
                })),
            },
        },
        steps: [],
        created: { time: 1641034800000, actor: null },
        lastModified: { time: 1641034800000, actor: null },
    });

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('displays workflow name when workflow has Home entrypoint', async () => {
        const actionRequest = createActionRequest();
        const workflow = createWorkflowMock([
            ActionWorkflowEntrypointType.Home,
            ActionWorkflowEntrypointType.EntityProfile,
        ]);

        mockUseListActionWorkflows.mockReturnValue({
            workflows: [workflow] as any,
            total: 1,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <WorkflowFormRequestItem actionRequest={actionRequest} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByText('Dataset Access Request')).toBeInTheDocument();
        });
        expect(screen.queryByText('unknown (deleted workflow)')).not.toBeInTheDocument();
    });

    it('displays workflow name for EntityProfile-only workflows (bug fix verification)', async () => {
        // This test verifies the bug fix works.
        // The workflow only has ENTITY_PROFILE entrypoint (no HOME),
        // but we should still display its name because we now fetch without entrypoint filtering.
        const actionRequest = createActionRequest();
        const workflow = createWorkflowMock([ActionWorkflowEntrypointType.EntityProfile]);

        mockUseListActionWorkflows.mockReturnValue({
            workflows: [workflow] as any,
            total: 1,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <WorkflowFormRequestItem actionRequest={actionRequest} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByText('Dataset Access Request')).toBeInTheDocument();
        });
        expect(screen.queryByText('unknown (deleted workflow)')).not.toBeInTheDocument();
    });

    it('displays "unknown (deleted workflow)" when workflow does not exist', async () => {
        const actionRequest = createActionRequest();

        mockUseListActionWorkflows.mockReturnValue({
            workflows: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <WorkflowFormRequestItem actionRequest={actionRequest} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByText('unknown (deleted workflow)')).toBeInTheDocument();
        });
    });

    it('returns null when no workflowFormRequest params', () => {
        const actionRequest = createActionRequest({
            params: {},
        });

        mockUseListActionWorkflows.mockReturnValue({
            workflows: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const { container } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <WorkflowFormRequestItem actionRequest={actionRequest} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container.firstChild).toBeNull();
    });
});
