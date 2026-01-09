import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

export interface ModuleProps {
    module: PageModuleFragment;
    position: ModulePositionInput;
    onClick?: () => void;
    showViewAll?: boolean;
}

export enum ModuleSize {
    FULL = 'full',
    HALF = 'half',
    THIRD = 'third',
}

export interface ModuleContextType {
    // Reloading
    isReloading: boolean;
    onReloadingFinished: () => void;
    size?: ModuleSize;
}
