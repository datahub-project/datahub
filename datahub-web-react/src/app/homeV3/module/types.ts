// TODO: adapt to DataHubPageModuleProperties
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

// the current props are just to draft some components
export interface ModuleProps {
    module: PageModuleFragment;
    position: ModulePositionInput;
}
