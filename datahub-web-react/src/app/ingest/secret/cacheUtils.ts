import { ListSecretsDocument, ListSecretsQuery } from '../../../graphql/ingestion.generated';

export const removeSecretFromListSecretsCache = (urn, client, page, pageSize) => {
    const currData: ListSecretsQuery | null = client.readQuery({
        query: ListSecretsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
    });

    const newSecrets = [...(currData?.listSecrets?.secrets || []).filter((secret) => secret.urn !== urn)];

    client.writeQuery({
        query: ListSecretsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
        data: {
            listSecrets: {
                start: currData?.listSecrets?.start || 0,
                count: (currData?.listSecrets?.count || 1) - 1,
                total: (currData?.listSecrets?.total || 1) - 1,
                secrets: newSecrets,
            },
        },
    });
};

export const addSecretToListSecretsCache = (secret, client, pageSize) => {
    const currData: ListSecretsQuery | null = client.readQuery({
        query: ListSecretsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
    });

    const newSecrets = [secret, ...(currData?.listSecrets?.secrets || [])];

    client.writeQuery({
        query: ListSecretsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
        data: {
            listSecrets: {
                start: currData?.listSecrets?.start || 0,
                count: (currData?.listSecrets?.count || 1) + 1,
                total: (currData?.listSecrets?.total || 1) + 1,
                secrets: newSecrets,
            },
        },
    });
};

export const updateSecretInListSecretsCache = (updatedSecret, client, pageSize, page) => {
    const currData: ListSecretsQuery | null = client.readQuery({
        query: ListSecretsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
    });

    const updatedSecretIndex = (currData?.listSecrets?.secrets || [])
        .map((secret, index) => {
            if (secret.urn === updatedSecret.urn) {
                return index;
            }
            return -1;
        })
        .find((index) => index !== -1);

    if (updatedSecretIndex !== undefined) {
        const newSecrets = (currData?.listSecrets?.secrets || []).map((secret, index) => {
            return index === updatedSecretIndex ? updatedSecret : secret;
        });

        client.writeQuery({
            query: ListSecretsDocument,
            variables: {
                input: {
                    start: (page - 1) * pageSize,
                    count: pageSize,
                },
            },
            data: {
                listSecrets: {
                    start: currData?.listSecrets?.start || 0,
                    count: currData?.listSecrets?.count || 1,
                    total: currData?.listSecrets?.total || 1,
                    secrets: newSecrets,
                },
            },
        });
    }
};

export const clearSecretListCache = (client) => {
    // Remove any caching of 'listSecrets'
    client.cache.evict({ id: 'ROOT_QUERY', fieldName: 'listSecrets' });
};
