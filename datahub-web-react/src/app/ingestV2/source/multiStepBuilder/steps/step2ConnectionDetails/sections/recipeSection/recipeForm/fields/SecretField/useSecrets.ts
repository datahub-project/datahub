import { useMemo } from 'react';

import { useListSecretsQuery } from '@graphql/ingestion.generated';

export function useSecrets() {
    const { data, refetch: refetchSecrets } = useListSecretsQuery({
        variables: {
            input: {
                start: 0,
                count: 1000, // get all secrets
            },
        },
        nextFetchPolicy: 'cache-first',
    });

    const secrets = useMemo(() => {
        const fetchedSecrets = data?.listSecrets?.secrets || [];
        return [...fetchedSecrets].sort((secretA, secretB) => secretA.name.localeCompare(secretB.name));
    }, [data?.listSecrets?.secrets]);

    return { secrets, refetchSecrets };
}
