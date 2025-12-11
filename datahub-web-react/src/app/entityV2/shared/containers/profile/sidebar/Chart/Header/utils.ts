/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ChartProperties } from '@types';

// This util is used for Charts and Dashboards who get last updated differently than others
export function getLastUpdatedMs(
    properties: Pick<ChartProperties, 'lastModified' | 'lastRefreshed'> | null | undefined,
): number | undefined {
    return properties?.lastRefreshed || properties?.lastModified?.time || undefined;
}
