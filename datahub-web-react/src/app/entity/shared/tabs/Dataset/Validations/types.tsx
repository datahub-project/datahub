import { Assertion, AssertionType } from '../../../../../../types.generated';

export type AssertionStatusSummary = {
    passing: number;
    failing: number;
    erroring: number;
    total: number; // Total assertions with at least 1 run.
    totalAssertions: number;
};

/**
 * A group of assertions related by their logical type or category.
 */
export type AssertionGroup = {
    name: string;
    icon: React.ReactNode;
    description?: string;
    assertions: Assertion[];
    summary: AssertionStatusSummary;
    type: AssertionType;
};
