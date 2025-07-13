import { useCallback } from 'react';

import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment, useUpsertPageTemplateMutation } from '@graphql/template.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { PageTemplateScope, PageTemplateSurfaceType } from '@types';

export function useTemplateOperations() {
    const [upsertPageTemplateMutation] = useUpsertPageTemplateMutation();
    const [updateUserHomePageSettings] = useUpdateUserHomePageSettingsMutation();

    // Helper function to update template state with a new module
    const updateTemplateWithModule = useCallback(
        (
            templateToUpdate: PageTemplateFragment | null,
            module: PageModuleFragment,
            position: ModulePositionInput,
        ): PageTemplateFragment | null => {
            if (!templateToUpdate) return null;

            const newTemplate = { ...templateToUpdate };
            const newRows = [...(newTemplate.properties?.rows || [])];

            if (position.rowIndex === undefined) {
                // Add to new row at the end
                newRows.push({
                    modules: [module],
                });
            } else {
                // Add to existing row
                const { rowIndex } = position;
                if (rowIndex >= newRows.length) {
                    // Create new row if index is out of bounds
                    newRows.push({
                        modules: [module],
                    });
                } else {
                    const row = { ...newRows[rowIndex] };
                    const newModules = [...(row.modules || [])];

                    if (position.rowSide === 'left') {
                        newModules.unshift(module);
                    } else {
                        newModules.push(module);
                    }

                    row.modules = newModules;
                    newRows[rowIndex] = row;
                }
            }

            newTemplate.properties = {
                ...newTemplate.properties,
                rows: newRows,
            };

            return newTemplate;
        },
        [],
    );

    // Helper function to remove a module from template
    const removeModuleFromTemplate = useCallback(
        (
            templateToUpdate: PageTemplateFragment | null,
            moduleUrn: string,
            position: ModulePositionInput,
        ): PageTemplateFragment | null => {
            if (!templateToUpdate) return null;

            const newTemplate = { ...templateToUpdate };
            const newRows = [...(newTemplate.properties?.rows || [])];

            if (position.rowIndex === undefined || position.rowIndex >= newRows.length) {
                // Invalid position, return original template
                return templateToUpdate;
            }

            const { rowIndex, moduleIndex } = position;
            const row = { ...newRows[rowIndex] };
            const newModules = [...(row.modules || [])];

            // Use moduleIndex for precise removal if available, otherwise fall back to URN search
            if (moduleIndex !== undefined && moduleIndex >= 0 && moduleIndex < newModules.length) {
                // Verify the module at this index matches the expected URN as a safety check
                if (newModules[moduleIndex].urn === moduleUrn) {
                    newModules.splice(moduleIndex, 1);
                } else {
                    // Safety check failed, fall back to URN search
                    const foundIndex = newModules.findIndex((module) => module.urn === moduleUrn);
                    if (foundIndex !== -1) {
                        newModules.splice(foundIndex, 1);
                    } else {
                        // Module not found, return original template
                        return templateToUpdate;
                    }
                }
            } else {
                // Fall back to URN search for backwards compatibility
                const foundIndex = newModules.findIndex((module) => module.urn === moduleUrn);
                if (foundIndex === -1) {
                    // Module not found, return original template
                    return templateToUpdate;
                }
                newModules.splice(foundIndex, 1);
            }

            // If the row is now empty, remove the entire row
            if (newModules.length === 0) {
                newRows.splice(rowIndex, 1);
            } else {
                row.modules = newModules;
                newRows[rowIndex] = row;
            }

            newTemplate.properties = {
                ...newTemplate.properties,
                rows: newRows,
            };

            return newTemplate;
        },
        [],
    );

    // Helper function to upsert template
    const upsertTemplate = useCallback(
        (
            templateToUpsert: PageTemplateFragment | null,
            isPersonal: boolean,
            personalTemplate: PageTemplateFragment | null,
        ) => {
            if (!templateToUpsert) {
                console.error('Template is required for upsert');
                return Promise.reject(new Error('Template is required for upsert'));
            }

            const isCreatingPersonalTemplate = isPersonal && !personalTemplate;

            const input = {
                urn: isCreatingPersonalTemplate ? undefined : templateToUpsert.urn || undefined, // undefined for create
                rows:
                    templateToUpsert.properties?.rows?.map((row) => ({
                        modules: row.modules?.map((module) => module.urn) || [],
                    })) || [],
                scope: isPersonal ? PageTemplateScope.Personal : PageTemplateScope.Global,
                surfaceType: PageTemplateSurfaceType.HomePage,
            };

            return upsertPageTemplateMutation({
                variables: { input },
            }).then(({ data }) => {
                if (isCreatingPersonalTemplate && data?.upsertPageTemplate.urn) {
                    updateUserHomePageSettings({ variables: { input: { pageTemplate: data.upsertPageTemplate.urn } } });
                }
            });
        },
        [upsertPageTemplateMutation, updateUserHomePageSettings],
    );

    return {
        updateTemplateWithModule,
        removeModuleFromTemplate,
        upsertTemplate,
    };
}
