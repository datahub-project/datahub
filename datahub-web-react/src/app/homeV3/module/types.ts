import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType } from '@types';

export interface ModuleProps {
    module: PageModuleFragment;
    position: ModulePositionInput;
    onClick?: () => void;
    showViewAll?: boolean;
}

export interface LoadedModulesEntry {
    urn: string;
    type: DataHubPageModuleType;
}

export interface ModulesContextType {
    // Modules reloading
    reloadModules: (moduleTypes: DataHubPageModuleType[], interval?: number) => void;
    shouldModuleBeReloaded: (moduleType: DataHubPageModuleType, moduleUrn?: string) => boolean;
    markModulesAsReloaded: (moduleType: DataHubPageModuleType, moduleUrn?: string) => void;
}

export interface ModuleContextType {
    // Reloading
    isReloading: boolean;
    onReloadingFinished: () => void;
}
