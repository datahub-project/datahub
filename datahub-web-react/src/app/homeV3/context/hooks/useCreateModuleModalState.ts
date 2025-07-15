import { useCallback, useState } from 'react';

import { CreateModuleModalState } from '@app/homeV3/context/types';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { DataHubPageModuleType } from '@types';

export function useCreateModuleModalState(): CreateModuleModalState {
    const [moduleType, setModuleType] = useState<DataHubPageModuleType | null>(null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const [position, setPosition] = useState<ModulePositionInput | null>(null);

    const open = useCallback((moduleTypeToCreate: DataHubPageModuleType, positionToCreate: ModulePositionInput) => {
        setModuleType(moduleTypeToCreate);
        setIsOpen(true);
        setPosition(positionToCreate);
    }, []);

    const close = useCallback(() => {
        setModuleType(null);
        setPosition(null);
        setIsOpen(false);
    }, []);

    return {
        moduleType,
        isOpen,
        position,
        open,
        close,
    };
}
