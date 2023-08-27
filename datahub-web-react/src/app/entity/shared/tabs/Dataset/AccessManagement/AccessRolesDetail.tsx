export default function handleExternalRoles(externalRoles, loggedInUser) {
    const accessRoles = new Array<any>();
    if (
        externalRoles?.dataset?.access &&
        externalRoles?.dataset?.access.roles &&
        externalRoles?.dataset?.access.roles.length > 0
    ) {
        externalRoles?.dataset?.access?.roles?.forEach((userRoles) => {
            const arr = {
                name: userRoles?.role?.properties?.name || ' ',
                description: userRoles?.role?.properties?.description || ' ',
                accesstype: userRoles?.role?.properties?.type || ' ',
                access:
                    (userRoles?.role?.actors?.users &&
                        userRoles?.role?.actors?.users.length > 0 &&
                        userRoles?.role?.actors?.users?.some(
                            (user) => user.user.urn === loggedInUser?.me?.corpUser.urn,
                        )) ||
                    false,
                url: userRoles?.role?.properties?.requestUrl || window.location.href,
            };
            accessRoles.push(arr);
        });
    }

    return accessRoles;
}
