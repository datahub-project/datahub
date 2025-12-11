/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Assertion, AssertionType } from '@types';

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
