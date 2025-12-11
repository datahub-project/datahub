/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

export interface ModuleProps {
    module: PageModuleFragment;
    position: ModulePositionInput;
    onClick?: () => void;
    showViewAll?: boolean;
}

export interface ModuleContextType {
    // Reloading
    isReloading: boolean;
    onReloadingFinished: () => void;
}
