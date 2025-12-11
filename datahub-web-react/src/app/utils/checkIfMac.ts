/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export function checkIfMac(): boolean {
    return (navigator as any).userAgentData
        ? (navigator as any).userAgentData.platform.toLowerCase().includes('mac')
        : navigator.userAgent.toLowerCase().includes('mac');
}
