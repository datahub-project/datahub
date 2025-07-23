import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

export interface ModuleProps {
    module: PageModuleFragment;
    position: ModulePositionInput;
    onClick?: () => void;
}
