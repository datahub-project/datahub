/**
 * Test Constants
 * Centralized constants for tests to avoid magic numbers
 * 
 * Usage in Cypress tests:
 *   cy.wait(TEST_TIMEOUTS.MEDIUM_WAIT); // Instead of cy.wait(2000)
 */

// Timeout and wait constants (in milliseconds)
export const TEST_TIMEOUTS = {
    SHORT_WAIT: 500,
    DEFAULT_WAIT: 1000,
    MEDIUM_WAIT: 2000, // Common wait time for async operations (e.g., cy.wait(2000))
    LONG_WAIT: 5000,
} as const;

// Progress percentage constants
export const PROGRESS_PERCENTAGES = {
    START: 10,
    MID: 50,
    NEAR_COMPLETE: 90,
    COMPLETE: 100,
} as const;

// Query and pagination constants
export const QUERY_LIMITS = {
    DEFAULT_COUNT: 1000,
    BATCH_SIZE: 50,
} as const;

// Validation limits
export const VALIDATION_LIMITS = {
    MAX_ENTITY_NAME_LENGTH: 100,
    MAX_PARENT_NODES: 10,
    MAX_HIERARCHY_DEPTH: 10,
    MAX_FILE_SIZE_MB: 10,
} as const;
