import { useListDomainsQuery } from '../../graphql/domain.generated';

interface Props {
    parentDomain?: string;
    skip?: boolean;
}

export default function useListDomains({ parentDomain, skip }: Props) {
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

    return { data, error, loading, refetch };
}
