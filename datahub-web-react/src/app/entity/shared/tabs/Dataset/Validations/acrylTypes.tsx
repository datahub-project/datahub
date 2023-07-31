import { Assertion } from '../../../../../../types.generated';

/**
 * A summary for a group of assertions.
 */
export type AssertionGroupSummary = {
    totalAssertions: number;
    totalRuns: number;
    failedRuns: number;
    succeededRuns: number;
    erroredRuns: number;
};

/**
 * A group of assertions related by their logical type or category.
 */
export type AssertionGroup = {
    name: string;
    icon: React.ReactNode;
    description?: string;
    assertions: Assertion[];
    summary: AssertionGroupSummary;
};
