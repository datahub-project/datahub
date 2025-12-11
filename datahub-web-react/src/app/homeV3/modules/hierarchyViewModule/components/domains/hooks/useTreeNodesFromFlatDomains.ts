/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import {
    convertDomainToTreeNode,
    unwrapFlatDomainsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';

import { Domain } from '@types';

export default function useTreeNodesFromFlatDomains(domains: Domain[] | undefined, shouldUnwrapParents = true) {
    return useMemo(() => {
        if (shouldUnwrapParents) return unwrapFlatDomainsToTreeNodes(domains ?? []);
        return domains?.map((domain) => convertDomainToTreeNode(domain));
    }, [domains, shouldUnwrapParents]);
}
