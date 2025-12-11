/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';

export default function useGlossaryNodesAndTermsByUrns(glossaryNodesAndTermsUrns: string[]) {
    const { isReloading, onReloadingFinished } = useModuleContext();

    return useGlossaryNodesAndTerms({
        glossaryNodesAndTermsUrns,
        count: glossaryNodesAndTermsUrns.length,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished(),
    });
}
