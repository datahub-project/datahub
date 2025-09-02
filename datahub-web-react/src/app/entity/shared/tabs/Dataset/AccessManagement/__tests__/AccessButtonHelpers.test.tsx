import { createAccessButtonHelpersTestSuite } from '@app/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers.testUtils';

/**
 * Tests for Entity (v1) AccessButtonHelpers
 * Uses shared test suite to ensure consistency across implementations
 */
createAccessButtonHelpersTestSuite('Entity', async () => {
    const module = await import('@app/entity/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers');
    return module;
});
