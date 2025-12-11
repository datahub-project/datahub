/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { createAccessButtonHelpersTestSuite } from '@utils/test-utils/createAccessButtonHelpersTestSuite';

/**
 * Tests for EntityV2 AccessButtonHelpers
 * Uses shared test suite to ensure consistency across implementations
 */
createAccessButtonHelpersTestSuite('EntityV2', async () => {
    const module = await import('@app/entityV2/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers');
    return module;
});
