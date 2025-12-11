/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

export interface FineGrainedOperation {
    inputColumns?: Array<[string, string]>; // [table, column]
    outputColumns?: Array<[string, string]>;
    transformOperation?: string;
}

interface EntitySidebarContextProps {
    width: number;
    setSidebarClosed: (isClosed: boolean) => void;
    isClosed: boolean;
    fineGrainedOperations?: FineGrainedOperation[]; // For query entities in lineage, when a column is selected
    forLineage?: boolean;
    separateSiblings?: boolean;
}

export const entitySidebarContextDefaults: EntitySidebarContextProps = {
    width: window.innerWidth * 0.3,
    setSidebarClosed: () => {},
    isClosed: false,
};

const EntitySidebarContext = React.createContext<EntitySidebarContextProps>({
    ...entitySidebarContextDefaults,
});

export default EntitySidebarContext;
