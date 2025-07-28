import React, { useCallback, useState } from 'react';

import ChildrenLoaderContext from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderContext';
import { ChildrenLoaderMetadata, MetadataMap } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { DEFAULT_NUMBER_OF_CHILDREN_TO_LOAD } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

interface Props {
    onLoadFinished: (nodes: TreeNode[], metadata: ChildrenLoaderMetadata, parentValue: string) => void;
    maxNumberOfChildrenToLoad?: number;
}

export function ChildrenLoaderProvider({
    children,
    onLoadFinished,
    maxNumberOfChildrenToLoad = DEFAULT_NUMBER_OF_CHILDREN_TO_LOAD,
}: React.PropsWithChildren<Props>) {
    const [metadataMap, setMetadataMap] = useState<MetadataMap>({});

    const get = useCallback((value: string) => metadataMap[value], [metadataMap]);

    const upsert = useCallback(
        (value: string, metadataChanges: ChildrenLoaderMetadata) =>
            setMetadataMap((prev) => ({ ...prev, ...{ [value]: { ...(prev[value] ?? {}), ...metadataChanges } } })),
        [],
    );

    const onLoad = useCallback(
        (nodes: TreeNode[], metadata: ChildrenLoaderMetadata, parentValue: string) => {
            upsert(parentValue, metadata);
            const updatedMetadata = { ...(get(parentValue) ?? {}), ...metadata };
            onLoadFinished(nodes, updatedMetadata, parentValue);
        },
        [onLoadFinished, upsert, get],
    );

    return (
        <ChildrenLoaderContext.Provider value={{ get, upsert, onLoad, maxNumberOfChildrenToLoad }}>
            {children}
        </ChildrenLoaderContext.Provider>
    );
}
