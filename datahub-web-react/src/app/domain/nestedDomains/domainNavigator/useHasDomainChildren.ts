import { useEffect } from 'react';
import { useGetDomainChildrenCountLazyQuery } from '../../../../graphql/domain.generated';
import { useDomainsContext } from '../../DomainsContext';

interface Props {
    domainUrn: string;
    numDomainChildren: number; // number that comes from parent query to render this domain
}

export default function useHasDomainChildren({ domainUrn, numDomainChildren }: Props) {
    const { parentDomainsToUpate, setParentDomainsToUpdate } = useDomainsContext();
    const [getDomainChildrenCount, { data: childrenData }] = useGetDomainChildrenCountLazyQuery();

    useEffect(() => {
        let timer;
        // fetch updated children count to determine if we show triangle toggle
        if (parentDomainsToUpate.includes(domainUrn)) {
            timer = setTimeout(() => {
                getDomainChildrenCount({ variables: { urn: domainUrn } });
                setParentDomainsToUpdate(parentDomainsToUpate.filter((urn) => urn !== domainUrn));
            }, 2000);
        }
        return () => {
            if (timer) window.clearTimeout(timer);
        };
    }, [domainUrn, getDomainChildrenCount, parentDomainsToUpate, setParentDomainsToUpdate]);

    return childrenData ? !!childrenData.domain?.children?.total : !!numDomainChildren;
}
