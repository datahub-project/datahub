/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ColumnEdge } from '@app/lineage/types';

import { SchemaField, SchemaFieldRef } from '@types';

export const LineageExplorerContext = React.createContext<LineageExplorerContextType>({
    expandTitles: false,
    showColumns: false,
    collapsedColumnsNodes: null,
    setCollapsedColumnsNodes: null,
    fineGrainedMap: { forward: [], reverse: [] },
    selectedField: null,
    setSelectedField: () => {},
    highlightedEdges: [],
    setHighlightedEdges: () => {},
    visibleColumnsByUrn: {},
    setVisibleColumnsByUrn: () => {},
    columnsByUrn: {},
    setColumnsByUrn: () => {},
    refetchCenterNode: () => {},
});

type LineageExplorerContextType = {
    expandTitles: boolean;
    showColumns: boolean;
    collapsedColumnsNodes: any;
    setCollapsedColumnsNodes: any;
    fineGrainedMap: any;
    selectedField: SchemaFieldRef | null;
    setSelectedField: (field: SchemaFieldRef | null) => void;
    highlightedEdges: ColumnEdge[];
    setHighlightedEdges: React.Dispatch<React.SetStateAction<ColumnEdge[]>>;
    visibleColumnsByUrn: any;
    setVisibleColumnsByUrn: any;
    columnsByUrn: Record<string, SchemaField[]>;
    setColumnsByUrn: React.Dispatch<React.SetStateAction<Record<string, SchemaField[]>>>;
    refetchCenterNode: () => void;
};
