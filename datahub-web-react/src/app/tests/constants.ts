import { TestCategoryType } from './types';

export const METADATA_TESTS_DOC_URL = 'https://datahubproject.io/docs/tests/metadata-tests/';
export const DEFAULT_TESTS_PAGE_SIZE = 1000; // Max Tests size is 1000. If a company has more than 1000 tests this will break.
export const PLACEHOLDER_TEST_URN = 'placeholder-test-urn';

/**
 * A set of well-supported, built-in Metadata Tests categories.
 */
export enum TestCategory {
    DATA_GOVERNANCE = 'Data Governance',
    DATA_QUALITY = 'Data Quality',
    DATA_DISCOVERY = 'Data Discovery',
    DATA_COST = 'Data Cost',
    CUSTOM = 'Custom',
}

/**
 * Information about a well-supported set of Test Categories.
 */
export const TEST_CATEGORIES: TestCategoryType[] = [
    {
        name: TestCategory.DATA_GOVERNANCE,
        description:
            'Tests for monitoring and enforcing Data Governance & Compliance policies, for example those related to Data Ownership and Classification.',
    },
    {
        name: TestCategory.DATA_QUALITY,
        description:
            'Tests for monitoring and enforcing Data Quality policies, for example those related to data Assertions, Incidents, & usage.',
    },
    {
        name: TestCategory.DATA_DISCOVERY,
        description:
            'Tests for enriching your data assets for the purpose of improving data discoverability, for example by automatically applying Tags, Domains, and Glossary Terms.',
    },
    {
        name: TestCategory.DATA_COST,
        description: 'Tests for identifying costly data assets within your organization.',
    },
    {
        name: TestCategory.CUSTOM,
        description: 'A custom category for your Metadata Tests',
    },
];

/**
 * Default test category selected when one is not provided (or new Test)
 */
export const DEFAULT_TEST_CATEGORY = TestCategory.DATA_GOVERNANCE;

/**
 * A map of Test Category name to the other well-supported attributes about it.
 */
export const TEST_CATEGORY_NAME_TO_INFO = new Map<string, TestCategoryType>();
TEST_CATEGORIES.forEach((category) => TEST_CATEGORY_NAME_TO_INFO.set(category.name, category));
