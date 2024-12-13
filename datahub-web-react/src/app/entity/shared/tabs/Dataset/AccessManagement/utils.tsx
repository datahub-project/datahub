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
