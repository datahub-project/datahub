/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PageTemplateSurfaceType } from '@types';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export default function useIsTemplateEditable(templateType: PageTemplateSurfaceType) {
    // Editing is disabled in OSS
    return false;
}
