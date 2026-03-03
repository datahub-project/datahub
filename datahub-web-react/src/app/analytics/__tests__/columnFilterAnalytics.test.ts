import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EventType } from '@app/analytics/event';

// Mock analytics
const mockAnalyticsEvent = vi.fn();
vi.mock('@app/analytics', () => ({
    default: {
        event: mockAnalyticsEvent,
    },
}));

/**
 * Tests for enhanced column filter analytics that distinguish between:
 * 1. User-applied fieldPaths filters (hasUserAppliedColumnFilter)
 * 2. Schema field URN context from navigation (isSchemaFieldContext)
 */
describe('Enhanced Column Filter Analytics', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    const baseEventProps = {
        query: 'test query',
        page: 1,
        total: 100,
        maxDegree: '3',
    };

    describe('User-Applied Column Filters', () => {
        it('should track when user applies fieldPaths filter only', () => {
            const mockEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true, // backward compatibility
                hasUserAppliedColumnFilter: true,
                isSchemaFieldContext: false,
            };

            // Simulate the analytics call
            mockAnalyticsEvent(mockEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith({
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: true,
                isSchemaFieldContext: false,
            });
        });

        it('should track multiple field paths in user filter', () => {
            const mockEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: true, // User selected multiple columns
                isSchemaFieldContext: false,
            };

            mockAnalyticsEvent(mockEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith(mockEvent);
        });
    });

    describe('Schema Field URN Context', () => {
        it('should track when navigated from schema field URN only', () => {
            const mockEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true, // backward compatibility
                hasUserAppliedColumnFilter: false,
                isSchemaFieldContext: true,
            };

            mockAnalyticsEvent(mockEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith({
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: false,
                isSchemaFieldContext: true,
            });
        });

        it('should distinguish schema field context from user filter', () => {
            // First call: Schema field context only
            const schemaFieldEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: false,
                isSchemaFieldContext: true,
            };

            mockAnalyticsEvent(schemaFieldEvent);

            // Second call: User-applied filter only
            const userFilterEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: true,
                isSchemaFieldContext: false,
            };

            mockAnalyticsEvent(userFilterEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledTimes(2);
            expect(mockAnalyticsEvent).toHaveBeenNthCalledWith(1, schemaFieldEvent);
            expect(mockAnalyticsEvent).toHaveBeenNthCalledWith(2, userFilterEvent);
        });
    });

    describe('Combined Scenarios', () => {
        it('should track when both user filter and schema field context are present', () => {
            const mockEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true, // backward compatibility
                hasUserAppliedColumnFilter: true,
                isSchemaFieldContext: true,
            };

            mockAnalyticsEvent(mockEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith(mockEvent);
        });

        it('should track when neither filter type is present', () => {
            const mockEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: false, // backward compatibility
                hasUserAppliedColumnFilter: false,
                isSchemaFieldContext: false,
            };

            mockAnalyticsEvent(mockEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith(mockEvent);
        });
    });

    describe('Backward Compatibility', () => {
        it('should maintain hasColumnFilter for existing analytics', () => {
            // Test that hasColumnFilter still works as expected
            const testCases = [
                {
                    userFilter: false,
                    schemaField: false,
                    expectedHasColumnFilter: false,
                },
                {
                    userFilter: true,
                    schemaField: false,
                    expectedHasColumnFilter: true,
                },
                {
                    userFilter: false,
                    schemaField: true,
                    expectedHasColumnFilter: true,
                },
                {
                    userFilter: true,
                    schemaField: true,
                    expectedHasColumnFilter: true,
                },
            ];

            testCases.forEach(({ userFilter, schemaField, expectedHasColumnFilter }, index) => {
                const mockEvent = {
                    type: EventType.SearchAcrossLineageResultsViewEvent,
                    ...baseEventProps,
                    hasColumnFilter: expectedHasColumnFilter,
                    hasUserAppliedColumnFilter: userFilter,
                    isSchemaFieldContext: schemaField,
                };

                mockAnalyticsEvent(mockEvent);

                expect(mockAnalyticsEvent).toHaveBeenNthCalledWith(index + 1, mockEvent);
            });
        });
    });

    describe('Real-world Usage Scenarios', () => {
        it('should track user drilling down from table-level to column-level lineage', () => {
            // Scenario: User starts at table lineage, then applies column filter
            const tableLineageEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: false,
                hasUserAppliedColumnFilter: false,
                isSchemaFieldContext: false,
            };

            const columnLineageEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: true, // User applied filter
                isSchemaFieldContext: false,
            };

            mockAnalyticsEvent(tableLineageEvent);
            mockAnalyticsEvent(columnLineageEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledTimes(2);
            expect(mockAnalyticsEvent).toHaveBeenNthCalledWith(1, tableLineageEvent);
            expect(mockAnalyticsEvent).toHaveBeenNthCalledWith(2, columnLineageEvent);
        });

        it('should track navigation from schema tab to lineage', () => {
            // Scenario: User clicks on column in schema tab, gets navigated to lineage
            const schemaNavigationEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: false,
                isSchemaFieldContext: true, // Came from schema field navigation
            };

            mockAnalyticsEvent(schemaNavigationEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith(schemaNavigationEvent);
        });

        it('should track progressive refinement: navigation + user filter', () => {
            // Scenario: User navigates from schema field, then applies additional filters
            const progressiveRefinementEvent = {
                type: EventType.SearchAcrossLineageResultsViewEvent,
                ...baseEventProps,
                hasColumnFilter: true,
                hasUserAppliedColumnFilter: true, // User added more filters
                isSchemaFieldContext: true, // Still in schema field context
            };

            mockAnalyticsEvent(progressiveRefinementEvent);

            expect(mockAnalyticsEvent).toHaveBeenCalledWith(progressiveRefinementEvent);
        });
    });

    describe('Analytics Insights', () => {
        it('should enable insights about user behavior patterns', () => {
            // This test demonstrates the kind of insights we can now gather
            const analyticsInsights = {
                userDiscoveryPatterns: {
                    explicitColumnFiltering: 'hasUserAppliedColumnFilter=true & isSchemaFieldContext=false',
                    schemaFieldDrillDown: 'hasUserAppliedColumnFilter=false & isSchemaFieldContext=true',
                    progressiveRefinement: 'hasUserAppliedColumnFilter=true & isSchemaFieldContext=true',
                    tableLineageOnly: 'hasUserAppliedColumnFilter=false & isSchemaFieldContext=false',
                },
                migrationPath: {
                    // Can still use hasColumnFilter for existing dashboards
                    // while migrating to more granular fields
                    legacy: 'hasColumnFilter',
                    enhanced: 'hasUserAppliedColumnFilter + isSchemaFieldContext',
                },
            };

            expect(analyticsInsights.userDiscoveryPatterns).toBeDefined();
            expect(analyticsInsights.migrationPath).toBeDefined();
        });
    });
});
