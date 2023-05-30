export const clearRoleListCache = (client) => {
    // Remove any caching of 'listRoles'
    client.cache.evict({ id: 'ROOT_QUERY', fieldName: 'listRoles' });
};
