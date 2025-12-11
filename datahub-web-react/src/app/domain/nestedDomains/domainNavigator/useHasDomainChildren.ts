/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { useDomainsContext } from '@app/domain/DomainsContext';

import { useGetDomainChildrenCountLazyQuery } from '@graphql/domain.generated';

interface Props {
    domainUrn: string;
    numDomainChildren: number; // number that comes from parent query to render this domain
}

export default function useHasDomainChildren({ domainUrn, numDomainChildren }: Props) {
    const { parentDomainsToUpdate, setParentDomainsToUpdate } = useDomainsContext();
    const [getDomainChildrenCount, { data: childrenData }] = useGetDomainChildrenCountLazyQuery();

    useEffect(() => {
        let timer;
        // fetch updated children count to determine if we show triangle toggle
        if (parentDomainsToUpdate.includes(domainUrn)) {
            timer = setTimeout(() => {
                getDomainChildrenCount({ variables: { urn: domainUrn } });
                setParentDomainsToUpdate(parentDomainsToUpdate.filter((urn) => urn !== domainUrn));
            }, 2000);
        }
        return () => {
            if (timer) window.clearTimeout(timer);
        };
    }, [domainUrn, getDomainChildrenCount, parentDomainsToUpdate, setParentDomainsToUpdate]);

    return childrenData ? !!childrenData.domain?.children?.total : !!numDomainChildren;
}
