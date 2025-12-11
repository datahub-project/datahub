/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export function handleAccessRoles(externalRoles) {
    const accessRoles = new Array<any>();
    if (
        externalRoles?.dataset?.access &&
        externalRoles?.dataset?.access?.roles &&
        externalRoles?.dataset?.access?.roles?.length > 0
    ) {
        externalRoles?.dataset?.access?.roles?.forEach((userRoles) => {
            const role = {
                name: userRoles?.role?.properties?.name || ' ',
                description: userRoles?.role?.properties?.description || ' ',
                accessType: userRoles?.role?.properties?.type || ' ',
                hasAccess: userRoles?.role?.isAssignedToMe,
                url: userRoles?.role?.properties?.requestUrl || undefined,
            };
            accessRoles.push(role);
        });
    }

    return accessRoles;
}
