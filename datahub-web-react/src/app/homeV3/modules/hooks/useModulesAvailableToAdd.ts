import { useMemo } from 'react';

import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import {
    MOCKED_ADMIN_CREATED_MODULES,
    MOCKED_CUSTOM_LARGE_MODULES,
    MOCKED_CUSTOM_MODULES,
} from '@app/homeV3/template/components/addModuleMenu/constants';

export default function useModulesAvailableToAdd(): ModulesAvailableToAdd {
    // TODO:: here we have to add logic with getting available modules
    return useMemo(() => {
        const customModules = MOCKED_CUSTOM_MODULES;
        const customLargeModules = MOCKED_CUSTOM_LARGE_MODULES;
        const adminCreatedModules = MOCKED_ADMIN_CREATED_MODULES;

        return {
            customModules,
            customLargeModules,
            adminCreatedModules,
        };
    }, []);
}
