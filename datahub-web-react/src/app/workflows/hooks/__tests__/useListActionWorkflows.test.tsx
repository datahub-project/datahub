import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    type UseListActionWorkflowsOptions,
    type WorkflowContext,
    getEntityProfileWorkflowContext,
    getEntryPointLabel,
    getHomePageWorkflowContext,
    useListActionWorkflows,
} from '@app/workflows/hooks/useListActionWorkflows';

import { ActionWorkflowFragment, useListActionWorkflowsQuery } from '@graphql/actionWorkflow.generated';
import { ActionWorkflowCategory, ActionWorkflowEntrypoint, ActionWorkflowEntrypointType, EntityType } from '@types';

// Mock the GraphQL hook
vi.mock('@graphql/actionWorkflow.generated', () => ({
    useListActionWorkflowsQuery: vi.fn(),
}));

const mockUseListActionWorkflowsQuery = vi.mocked(useListActionWorkflowsQuery);

describe('useListActionWorkflows', () => {
    const mockWorkflows = [
        {
            urn: 'urn:li:workflow:1',
            name: 'Test Workflow 1',
            categpory: ActionWorkflowCategory.Access,
            trigger: {
                form: {
                    entrypoints: [
                        {
                            type: ActionWorkflowEntrypointType.Home,
                            label: 'Home Label 1',
                        },
                        {
                            type: ActionWorkflowEntrypointType.EntityProfile,
                            label: 'Profile Label 1',
                        },
                    ],
                },
            },
        },
        {
            urn: 'urn:li:workflow:2',
            name: 'Test Workflow 2',
            category: ActionWorkflowCategory.Access,
            trigger: {
                form: {
                    entrypoints: [
                        {
                            type: ActionWorkflowEntrypointType.Home,
                            label: 'Home Label 2',
                        },
                    ],
                },
            },
        },
    ];

    const mockQueryResponse = {
        data: {
            listActionWorkflows: {
                workflows: mockWorkflows,
                total: 2,
            },
        },
        loading: false,
        error: undefined,
        refetch: vi.fn(),
    } as any;

    beforeEach(() => {
        vi.clearAllMocks();
        mockUseListActionWorkflowsQuery.mockReturnValue(mockQueryResponse);
    });

    describe('useListActionWorkflows hook', () => {
        it('should call GraphQL query with correct input for home context', () => {
            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
            };

            renderHook(() => useListActionWorkflows(options));

            expect(mockUseListActionWorkflowsQuery).toHaveBeenCalledWith({
                variables: {
                    input: {
                        start: 0,
                        count: 1000,
                        entrypointType: ActionWorkflowEntrypointType.Home,
                    },
                },
                skip: false,
                fetchPolicy: 'cache-first',
            });
        });

        it('should call GraphQL query with correct input for entity profile context', () => {
            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.EntityProfile,
                    entityType: EntityType.Dataset,
                    entityUrn: 'urn:li:dataset:test',
                },
                category: ActionWorkflowCategory.Access,
                customCategory: 'custom-type',
            };

            renderHook(() => useListActionWorkflows(options));

            expect(mockUseListActionWorkflowsQuery).toHaveBeenCalledWith({
                variables: {
                    input: {
                        start: 0,
                        count: 1000,
                        entrypointType: ActionWorkflowEntrypointType.EntityProfile,
                        entityType: EntityType.Dataset,
                        category: ActionWorkflowCategory.Access,
                        customCategory: 'custom-type',
                    },
                },
                skip: false,
                fetchPolicy: 'cache-first',
            });
        });

        it('should handle custom fetch policy', () => {
            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
                fetchPolicy: 'network-only',
            };

            renderHook(() => useListActionWorkflows(options));

            expect(mockUseListActionWorkflowsQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    fetchPolicy: 'network-only',
                }),
            );
        });

        it('should skip query when enabled is false', () => {
            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
                enabled: false,
            };

            renderHook(() => useListActionWorkflows(options));

            expect(mockUseListActionWorkflowsQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    skip: true,
                }),
            );
        });

        it('should return workflows and metadata', () => {
            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
            };

            const { result } = renderHook(() => useListActionWorkflows(options));

            expect(result.current).toEqual({
                workflows: mockWorkflows,
                total: 2,
                loading: false,
                error: undefined,
                refetch: expect.any(Function),
            });
        });

        it('should handle empty response gracefully', () => {
            mockUseListActionWorkflowsQuery.mockReturnValue({
                ...mockQueryResponse,
                data: undefined,
            });

            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
            };

            const { result } = renderHook(() => useListActionWorkflows(options));

            expect(result.current).toEqual({
                workflows: [],
                total: 0,
                loading: false,
                error: undefined,
                refetch: expect.any(Function),
            });
        });

        it('should handle loading state', () => {
            mockUseListActionWorkflowsQuery.mockReturnValue({
                ...mockQueryResponse,
                data: undefined,
                loading: true,
            });

            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
            };

            const { result } = renderHook(() => useListActionWorkflows(options));

            expect(result.current.loading).toBe(true);
        });

        it('should handle error state', () => {
            const mockError = {
                name: 'GraphQL Error',
                message: 'Something went wrong',
                graphQLErrors: [],
                networkError: null,
                extraInfo: {},
            } as any;

            mockUseListActionWorkflowsQuery.mockReturnValue({
                ...mockQueryResponse,
                data: undefined,
                error: mockError,
            });

            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
            };

            const { result } = renderHook(() => useListActionWorkflows(options));

            expect(result.current.error).toBe(mockError);
        });

        it('should memoize input correctly', () => {
            const options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
                category: ActionWorkflowCategory.Access,
            };

            const { rerender } = renderHook(() => useListActionWorkflows(options));

            // First call
            expect(mockUseListActionWorkflowsQuery).toHaveBeenCalledTimes(1);
            const firstCall = mockUseListActionWorkflowsQuery.mock.calls[0]?.[0];

            // Rerender with same props
            rerender();
            expect(mockUseListActionWorkflowsQuery).toHaveBeenCalledTimes(2);
            const secondCall = mockUseListActionWorkflowsQuery.mock.calls[1]?.[0];

            // Input object should be the same reference (memoized)
            expect(firstCall?.variables?.input).toBe(secondCall?.variables?.input);
        });

        it('should update input when dependencies change', () => {
            let options: UseListActionWorkflowsOptions = {
                context: {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                },
                category: ActionWorkflowCategory.Access,
            };

            const { rerender } = renderHook(() => useListActionWorkflows(options));

            // Change the type
            options = {
                ...options,
                category: ActionWorkflowCategory.Access,
            };

            rerender();

            // Should be called with updated input
            expect(mockUseListActionWorkflowsQuery).toHaveBeenLastCalledWith(
                expect.objectContaining({
                    variables: {
                        input: expect.objectContaining({
                            category: ActionWorkflowCategory.Access,
                        }),
                    },
                }),
            );
        });
    });

    describe('helper functions', () => {
        describe('getHomePageWorkflowContext', () => {
            it('should return correct context for home page', () => {
                const context = getHomePageWorkflowContext();

                expect(context).toEqual({
                    entrypointType: ActionWorkflowEntrypointType.Home,
                });
            });
        });

        describe('getEntityProfileWorkflowContext', () => {
            it('should return correct context with entityType only', () => {
                const context = getEntityProfileWorkflowContext(EntityType.Dataset);

                expect(context).toEqual({
                    entrypointType: ActionWorkflowEntrypointType.EntityProfile,
                    entityType: EntityType.Dataset,
                    entityUrn: undefined,
                });
            });

            it('should return correct context with entityType and entityUrn', () => {
                const entityUrn = 'urn:li:dataset:test';
                const context = getEntityProfileWorkflowContext(EntityType.Dataset, entityUrn);

                expect(context).toEqual({
                    entrypointType: ActionWorkflowEntrypointType.EntityProfile,
                    entityType: EntityType.Dataset,
                    entityUrn,
                });
            });

            it('should handle different entity types', () => {
                const context = getEntityProfileWorkflowContext(EntityType.Dashboard);

                expect(context).toEqual({
                    entrypointType: ActionWorkflowEntrypointType.EntityProfile,
                    entityType: EntityType.Dashboard,
                    entityUrn: undefined,
                });
            });
        });

        describe('getEntryPointLabel', () => {
            const workflow = {
                name: 'Default Workflow Name',
                trigger: {
                    form: {
                        entrypoints: [
                            {
                                type: ActionWorkflowEntrypointType.Home,
                                label: 'Home Entrypoint Label',
                            },
                            {
                                type: ActionWorkflowEntrypointType.EntityProfile,
                                label: 'Profile Entrypoint Label',
                            },
                        ],
                    },
                },
            } as ActionWorkflowFragment;

            it('should return entrypoint label when found', () => {
                const context: WorkflowContext = {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                };

                const label = getEntryPointLabel(workflow, context);

                expect(label).toBe('Home Entrypoint Label');
            });

            it('should return workflow name when entrypoint not found', () => {
                // Create a workflow that only has Home entrypoint, then search for EntityProfile
                const workflowWithOnlyHome = {
                    name: 'Default Workflow Name',
                    trigger: {
                        form: {
                            entrypoints: [
                                {
                                    type: ActionWorkflowEntrypointType.Home,
                                    label: 'Home Entrypoint Label',
                                },
                            ],
                        },
                    },
                } as ActionWorkflowFragment;

                const context: WorkflowContext = {
                    entrypointType: ActionWorkflowEntrypointType.EntityProfile,
                };

                const label = getEntryPointLabel(workflowWithOnlyHome, context);

                expect(label).toBe('Default Workflow Name');
            });

            it('should handle workflow with empty entrypoints', () => {
                const workflowWithoutEntrypoints = {
                    name: 'Workflow Without Entrypoints',
                    trigger: {
                        form: {
                            entrypoints: [] as ActionWorkflowEntrypoint[],
                        },
                    },
                } as ActionWorkflowFragment;

                const context: WorkflowContext = {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                };

                const label = getEntryPointLabel(workflowWithoutEntrypoints, context);

                expect(label).toBe('Workflow Without Entrypoints');
            });

            it('should handle entrypoint with empty label', () => {
                const workflowWithEmptyLabel = {
                    name: 'Workflow Name',
                    trigger: {
                        form: {
                            entrypoints: [
                                {
                                    type: ActionWorkflowEntrypointType.Home,
                                    label: '',
                                },
                            ],
                        },
                    },
                } as ActionWorkflowFragment;

                const context: WorkflowContext = {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                };

                const label = getEntryPointLabel(workflowWithEmptyLabel, context);

                expect(label).toBe('Workflow Name');
            });

            it('should handle null/undefined label', () => {
                const workflowWithNullLabel = {
                    name: 'Workflow Name',
                    trigger: {
                        form: {
                            entrypoints: [
                                {
                                    type: ActionWorkflowEntrypointType.Home,
                                },
                            ] as ActionWorkflowEntrypoint[],
                        },
                    },
                } as ActionWorkflowFragment;

                const context: WorkflowContext = {
                    entrypointType: ActionWorkflowEntrypointType.Home,
                };

                const label = getEntryPointLabel(workflowWithNullLabel, context);

                expect(label).toBe('Workflow Name');
            });
        });
    });
});
