import { TEST_CATEGORY_NAME_TO_INFO, TestCategory } from '@app/tests/constants';
import { TestCategoryGroupType } from '@app/tests/types';

import { Test } from '@types';

/**
 * Filter tests by a search string. Compares name, description, category.
 *
 * @param filterText the search text
 * @param tests the tests to filter
 */
export const filterTests = (filterText, tests: Test[]) => {
    if (!filterText) {
        return tests;
    }
    const lowerFilterText = filterText.toLowerCase();
    return tests.filter((test) => {
        return (
            test.name?.toLocaleLowerCase().includes(lowerFilterText) ||
            test.description?.toLocaleLowerCase().includes(lowerFilterText) ||
            test.category?.toLocaleLowerCase().includes(lowerFilterText)
        );
    });
};

/**
 * Attempts to retrieve a description for a Metadata Tests category. This only works for well-supported category types like
 * Governance, Quality, Discovery, and Cost.
 *
 * @param name the name of the category.
 */
const getTestCategoryDescription = (name: string): string | undefined => {
    const categoryInfo = TEST_CATEGORY_NAME_TO_INFO.get(name);
    return categoryInfo?.description;
};

/**
 * Groups a list of Metadata Tests based on the category name. By default, the groups are sorted by the number of tests inside of them.
 *
 * @param tests the tests to group
 */
export const groupTestsByCategory = (tests: Test[]): TestCategoryGroupType[] => {
    const categoryToTests = new Map<string, TestCategoryGroupType>();
    tests.forEach((test) => {
        const currentGroup = categoryToTests.get(test.category);
        if (currentGroup) {
            currentGroup.tests.push(test);
        } else {
            categoryToTests.set(test.category, {
                name: test.category,
                description: getTestCategoryDescription(test.category),
                tests: [test],
            });
        }
    });
    // Finally, return each group.
    return Array.from(categoryToTests.values()).sort((a, b) => b.tests.length - a.tests.length);
};

/**
 * Returns true if the category name is "well-supported" (e.g. a built in), false otherwise.
 *
 * @param categoryName the name of the category
 */
export const isSupportedCategory = (categoryName) => {
    return TEST_CATEGORY_NAME_TO_INFO.get(categoryName) !== undefined;
};

/**
 * Returns true if the category is considered a custom category.
 *
 * @param categoryName the name of a category
 */
export const isCustomCategory = (categoryName: string | undefined) => {
    return categoryName && (categoryName === TestCategory.CUSTOM || !isSupportedCategory(categoryName));
};
