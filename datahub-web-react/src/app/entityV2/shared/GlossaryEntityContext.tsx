/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity } from '@src/types.generated';

export interface GlossaryEntityContextType {
    isInGlossaryContext: boolean;
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    // Since we have glossary data in the profile and in the sidebar browser, we need to communicate to the
    // sidebar when to refetch for a given node or at the root level (if we're editing a term or node without a parent).
    // This will happen when you edit a name, move a term/group, create a new term/group, and delete a term/group
    urnsToUpdate: string[];
    setUrnsToUpdate: (updatdUrns: string[]) => void;
    isSidebarOpen: boolean;
    setIsSidebarOpen: (isOpen: boolean) => void;
    nodeToNewEntity: Record<string, Entity>;
    setNodeToNewEntity: React.Dispatch<React.SetStateAction<Record<string, Entity>>>;
    nodeToDeletedUrn: Record<string, string>;
    setNodeToDeletedUrn: React.Dispatch<React.SetStateAction<Record<string, string>>>;
}

export const GlossaryEntityContext = React.createContext<GlossaryEntityContextType>({
    isInGlossaryContext: false,
    entityData: null,
    setEntityData: () => {},
    urnsToUpdate: [],
    setUrnsToUpdate: () => {},
    isSidebarOpen: true,
    setIsSidebarOpen: () => {},
    nodeToNewEntity: {},
    setNodeToNewEntity: () => {},
    nodeToDeletedUrn: {},
    setNodeToDeletedUrn: () => {},
});

export const useGlossaryEntityData = () => {
    const {
        isInGlossaryContext,
        entityData,
        setEntityData,
        urnsToUpdate,
        setUrnsToUpdate,
        isSidebarOpen,
        setIsSidebarOpen,
        nodeToNewEntity,
        setNodeToNewEntity,
        nodeToDeletedUrn,
        setNodeToDeletedUrn,
    } = useContext(GlossaryEntityContext);
    return {
        isInGlossaryContext,
        entityData,
        setEntityData,
        urnsToUpdate,
        setUrnsToUpdate,
        isSidebarOpen,
        setIsSidebarOpen,
        nodeToNewEntity,
        setNodeToNewEntity,
        nodeToDeletedUrn,
        setNodeToDeletedUrn,
    };
};
