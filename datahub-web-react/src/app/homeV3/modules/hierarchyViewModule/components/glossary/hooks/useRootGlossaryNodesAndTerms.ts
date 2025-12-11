/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import useRootGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryNodes';
import useRootGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryTerms';

export default function useRootGlossaryNodesAndTerms() {
    const { glossaryNodes, loading: glossaryNodesLoading } = useRootGlossaryNodes();
    const { glossaryTerms, loading: glossaryTermsLoading } = useRootGlossaryTerms();

    const glossaryItems = useMemo(
        () => [...(glossaryNodes ?? []), ...(glossaryTerms ?? [])],
        [glossaryNodes, glossaryTerms],
    );

    return { glossaryItems, glossaryNodes, glossaryTerms, loading: glossaryNodesLoading || glossaryTermsLoading };
}
