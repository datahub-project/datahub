import type { AutomationCategoryType } from '@app/automations/types';

export const env = {
    hideSidebar: true,
    hideMetadataTests: true,
    hideActionStatus: true,
    actionStatusPollingInterval: 30000, // 30 seconds
};

export enum AutomationTypes {
    TEST = 'Test',
    ACTION = 'ActionPipeline',
    INGESTION = 'IngestionPipeline',
}

export enum AutomationStage {
    LIVE = 'live',
    BOOTSTRAP = 'bootstrap',
    ROLLBACK = 'rollback',
}

export enum AutomationActionStatus {
    RUNNING = 'RUNNING',
    STOPPED = 'STOPPED',
    SUCCEEDED = 'SUCCESS',
    FAILED = 'FAIL',
}

export enum AutomationStatus {
    ACTIVE = 'ACTIVE',
    INACTIVE = 'INACTIVE',
}

export enum AutomationCategory {
    DATA_GOVERNANCE = 'Data Governance',
    DATA_QUALITY = 'Data Quality',
    DATA_DISCOVERY = 'Data Discovery',
    DATA_COST = 'Data Cost',
    CUSTOM = 'Custom',
}

export const METADATA_TESTS_DOC_URL = 'https://datahubproject.io/docs/tests/metadata-tests/';
export const DEFAULT_TESTS_PAGE_SIZE = 1000; // Max Tests size is 1000. If a company has more than 1000 tests this will break.
export const PLACEHOLDER_TEST_URN = 'placeholder-test-urn';

/**
 * Information about a well-supported set of Test Categories.
 */
export const AUTOMATION_CATEGORIES: AutomationCategoryType[] = [
    {
        name: AutomationCategory.DATA_GOVERNANCE,
        description:
            'Tests for monitoring and enforcing Data Governance & Compliance policies, for example those related to Data Ownership and Classification.',
    },
    {
        name: AutomationCategory.DATA_QUALITY,
        description:
            'Tests for monitoring and enforcing Data Quality policies, for example those related to data Assertions, Incidents, & usage.',
    },
    {
        name: AutomationCategory.DATA_DISCOVERY,
        description:
            'Tests for enriching your data assets for the purpose of improving data discoverability, for example by automatically applying Tags, Domains, and Glossary Terms.',
    },
    {
        name: AutomationCategory.DATA_COST,
        description: 'Tests for identifying costly data assets within your organization.',
    },
    {
        name: AutomationCategory.CUSTOM,
        description: 'A custom category for your Metadata Tests',
    },
];

/**
 * Default test category selected when one is not provided (or new Test)
 */
export const DEFAULT_AUTOMATION_CATEGORY = AutomationCategory.DATA_GOVERNANCE;

/**
 * A map of Test Category name to the other well-supported attributes about it.
 */
export const AUTOMATION_CATEGORY_NAME_TO_INFO = new Map<string, AutomationCategoryType>();
AUTOMATION_CATEGORIES.forEach((category) => AUTOMATION_CATEGORY_NAME_TO_INFO.set(category.name, category));
