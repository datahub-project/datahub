import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { AndFilterInput } from '@types';

export interface ChildrenLoaderMetadata {
    totalNumberOfChildren?: number;
    numberOfLoadedChildren?: number;

    totalNumberOfRelatedEntities?: number;
    numberOfLoadedRelatedEntities?: number;
}

export interface ChildrenLoaderInputType {
    parentValue: string;
    metadata?: ChildrenLoaderMetadata;
    maxNumberToLoad: number;
    dependenciesIsLoading?: boolean;
    orFilters?: AndFilterInput[];
}

export interface ChildrenLoaderResultType {
    nodes?: TreeNode[];
    total?: number;
    loading: boolean;
}

export type ChildrenLoaderType = (input: ChildrenLoaderInputType) => ChildrenLoaderResultType;

export interface MetadataMap {
    [key: string]: ChildrenLoaderMetadata;
}

export interface ChildrenLoaderContextType {
    get: (value: string) => ChildrenLoaderMetadata | undefined;
    upsert: (value: string, metadata: ChildrenLoaderMetadata) => void;
    onLoad: (nodes: TreeNode[], metadata: ChildrenLoaderMetadata, parentValue: string) => void;

    maxNumberOfChildrenToLoad: number;
}
