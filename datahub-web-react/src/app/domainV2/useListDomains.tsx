import { useListDomainsQuery } from '../../graphql/domain.generated';
import { useSortedDomains } from './utils';

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
