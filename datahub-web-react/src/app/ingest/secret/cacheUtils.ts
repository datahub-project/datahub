import { ListSecretsDocument, ListSecretsQuery } from '@graphql/ingestion.generated';

export const clearSecretListCache = (client) => {
    // Remove any caching of 'listSecrets'
    client.cache.evict({ id: 'ROOT_QUERY', fieldName: 'listSecrets' });
};
