import { useCallback, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { ModuleModalState } from '@app/homeV3/context/types';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType } from '@types';

export function useModuleModalState(): ModuleModalState {
    const [moduleType, setModuleType] = useState<DataHubPageModuleType | null>(null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const [position, setPosition] = useState<ModulePositionInput | null>(null);
    const [isEditing, setIsEditing] = useState<boolean>(false);
    const [initialState, setInitialState] = useState<PageModuleFragment | null>(null);

    const open = useCallback((moduleTypeToCreate: DataHubPageModuleType, positionToCreate: ModulePositionInput) => {
        setModuleType(moduleTypeToCreate);
        setIsOpen(true);
        setPosition(positionToCreate);
        setIsEditing(false);
        setInitialState(null);

        analytics.event({
            type: EventType.HomePageTemplateModuleModalCreateOpen,
            moduleType: moduleTypeToCreate,
        });
    }, []);

    const openToEdit = useCallback(
        (
            moduleTypeToEdit: DataHubPageModuleType,
            currentData: PageModuleFragment,
            currentPosition: ModulePositionInput,
        ) => {
            setModuleType(moduleTypeToEdit);
            setIsEditing(true);
            setInitialState(currentData);
            setPosition(currentPosition);
            setIsOpen(true);

            analytics.event({
                type: EventType.HomePageTemplateModuleModalEditOpen,
                moduleType: moduleTypeToEdit,
            });
        },
        [],
    );

    const close = useCallback(() => {
        setModuleType(null);
        setPosition(null);
        setIsOpen(false);
        setIsEditing(false);
        setInitialState(null);

        if (moduleType) {
            analytics.event({
                type: EventType.HomePageTemplateModuleModalCancel,
                moduleType,
            });
        }
    }, [moduleType]);

    return {
        moduleType,
        isOpen,
        position,
        open,
        close,
        openToEdit,
        isEditing,
        initialState,
    };
}
