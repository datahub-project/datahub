import { CUSTOM_MODULE_TYPES } from '@app/homeV3/modules/constants';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';

export function getCustomGlobalModules(globalTemplate: PageTemplateFragment | null) {
    const customGlobalModules: PageModuleFragment[] = [];
    globalTemplate?.properties.rows.forEach((row) => {
        row.modules.forEach((module) => {
            if (CUSTOM_MODULE_TYPES.includes(module.properties.type)) {
                customGlobalModules.push(module);
            }
        });
    });

    return customGlobalModules;
}
