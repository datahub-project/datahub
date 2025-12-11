/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useSortedDomains } from '@app/domain/utils';

import { useListDomainsQuery } from '@graphql/domain.generated';

interface Props {
    parentDomain?: string;
    skip?: boolean;
    sortBy?: 'displayName';
}

export default function useListDomains({ parentDomain, skip, sortBy = 'displayName' }: Props) {
    const { data, error, loading, refetch } = useListDomainsQuery({
        skip,
        variables: {
            input: {
                start: 0,
                count: 1000, // don't paginate the home page, get all root level domains
                parentDomain,
            },
        },
        fetchPolicy: 'network-only', // always use network request first to populate cache
        nextFetchPolicy: 'cache-first', // then use cache after that so we can manipulate it
    });

    const sortedDomains = useSortedDomains(data?.listDomains?.domains, sortBy);

    return { data, sortedDomains, error, loading, refetch };
}
