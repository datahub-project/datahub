/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

import useGlossaryNodesAndTermsByUrns from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTermsByUrns';
import useGlossaryTreeNodesSorter from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryTreeNodesSorter';
import useRootGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryNodesAndTerms';
import useTreeNodesFromFlatGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useTreeNodesFromFlatGlossaryNodesAndTerms';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useTreeNodesFromGlossaryNodesAndTerms';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';

export default function useSelectableGlossaryTree(initialSelectedGlossaryNodesAndTermsUrns: string[]) {
    const nodesSorter = useGlossaryTreeNodesSorter();
    const tree = useTree(undefined, nodesSorter);

    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialSelectedGlossaryNodesAndTermsUrns ?? []);

    useEffect(() => {
        if (initialSelectedGlossaryNodesAndTermsUrns !== undefined) {
            setSelectedValues(initialSelectedGlossaryNodesAndTermsUrns);
            setIsInitialized(false);
        }
    }, [initialSelectedGlossaryNodesAndTermsUrns]);

    // Get initial nodes
    const {
        glossaryNodes: initialGlossaryNodes,
        glossaryTerms: initialGlossaryTerms,
        loading: initialDataLoading,
    } = useGlossaryNodesAndTermsByUrns(initialSelectedGlossaryNodesAndTermsUrns);
    const { treeNodes: initialSelectedTreeNodes } = useTreeNodesFromFlatGlossaryNodesAndTerms(
        initialGlossaryNodes,
        initialGlossaryTerms,
    );

    // Get root nodes
    const {
        glossaryNodes: rootGlossaryNodes,
        glossaryTerms: rootGlossaryTerms,
        loading: rootDataLoading,
    } = useRootGlossaryNodesAndTerms();
    const { treeNodes: rootTreeNodes } = useTreeNodesFromGlossaryNodesAndTerms(rootGlossaryNodes, rootGlossaryTerms);

    // Initialize nodes
    useEffect(() => {
        if (
            !isInitialized &&
            !initialDataLoading &&
            !rootDataLoading &&
            rootTreeNodes !== undefined &&
            initialSelectedTreeNodes !== undefined
        ) {
            tree.replace(rootTreeNodes);
            tree.merge(initialSelectedTreeNodes);
            setIsInitialized(true);
        }
    }, [tree, initialDataLoading, rootDataLoading, isInitialized, rootTreeNodes, initialSelectedTreeNodes]);

    return {
        tree,
        selectedValues,
        setSelectedValues,
        loading: !isInitialized,
    };
}
