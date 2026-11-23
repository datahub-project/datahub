import i18next from 'i18next';

import type { PillProps } from '@components/components/Pills/types';

import { TestResultType } from '@types';

type ResultPillProps = Required<Pick<PillProps, 'label' | 'color'>>;

// Red and green are left out so category pills never read as a pass/fail status.
const CATEGORY_COLORS: NonNullable<PillProps['color']>[] = ['violet', 'blue', 'yellow', 'gray'];

/**
 * Maps each distinct (free-form) test category to a pill color. Categories are sorted so the assignment
 * doesn't depend on row order, and colors only repeat once there are more categories than colors.
 */
export function getCategoryPillColors(categories: string[]): Map<string, NonNullable<PillProps['color']>> {
    const distinct = [...new Set(categories)].sort((a, b) => a.localeCompare(b));
    return new Map(distinct.map((category, i) => [category, CATEGORY_COLORS[i % CATEGORY_COLORS.length]]));
}

/**
 * Returns the label and color for the pill that represents a TestResultType.
 */
export function getResultPillProps(result: TestResultType): ResultPillProps {
    switch (result) {
        case TestResultType.Success:
            return { label: i18next.t('entity.profile.tests:resultText.passing'), color: 'green' };
        case TestResultType.Failure:
            return { label: i18next.t('entity.profile.tests:resultText.failing'), color: 'red' };
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
}
