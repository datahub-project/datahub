/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';

const PreviewContext = React.createContext<GenericEntityProperties | null>(null);
export default PreviewContext;

export function usePreviewData() {
    return React.useContext(PreviewContext);
}
