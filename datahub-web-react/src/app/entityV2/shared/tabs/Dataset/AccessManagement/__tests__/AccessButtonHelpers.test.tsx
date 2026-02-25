import { createAccessButtonHelpersTestSuite } from '@utils/test-utils/createAccessButtonHelpersTestSuite';

/**
 * Tests for EntityV2 AccessButtonHelpers
 * Uses shared test suite to ensure consistency across implementations
 */
createAccessButtonHelpersTestSuite('EntityV2', async () => {
    const module = await import('@app/entityV2/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers');
    return module;
});
