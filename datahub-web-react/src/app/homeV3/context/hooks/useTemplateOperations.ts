import { useCallback } from 'react';

import { insertModuleIntoRows } from '@app/homeV3/context/hooks/utils/moduleOperationsUtils';
import { DEFAULT_TEMPLATE_URN } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';
import useShowToast from '@app/homeV3/toast/useShowToast';

import {
    PageModuleFragment,
    PageTemplateFragment,
    useDeletePageTemplateMutation,
    useUpsertPageTemplateMutation,
} from '@graphql/template.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Helper function to find and remove a module from a modules array
const removeModuleFromArray = (
    modules: PageModuleFragment[],
    moduleUrn: string,
    moduleIndex?: number,
): PageModuleFragment[] => {
    const newModules = [...modules];

    // Use moduleIndex for precise removal if available
    if (moduleIndex !== undefined && moduleIndex >= 0 && moduleIndex < newModules.length) {
        // Verify the module at this index matches the expected URN as a safety check
        if (newModules[moduleIndex].urn === moduleUrn) {
            newModules.splice(moduleIndex, 1);
            return newModules;
        }
    }

    // Fall back to URN search
    const foundIndex = newModules.findIndex((module) => module.urn === moduleUrn);
    if (foundIndex !== -1) {
        newModules.splice(foundIndex, 1);
    }

    return newModules;
};

// Helper function to validate position for removal
const isValidRemovalPosition = (template: PageTemplateFragment | null, position: ModulePositionInput): boolean => {
    if (!template) return false;

    const rows = template.properties?.rows || [];
    const { rowIndex } = position;

    return rowIndex !== undefined && rowIndex >= 0 && rowIndex < rows.length;
};

export function useTemplateOperations(
    setPersonalTemplate: (template: PageTemplateFragment | null) => void,
    personalTemplate: PageTemplateFragment | null,
) {
    const [upsertPageTemplateMutation] = useUpsertPageTemplateMutation();
    const [updateUserHomePageSettings] = useUpdateUserHomePageSettingsMutation();
    const [deletePageTemplate] = useDeletePageTemplateMutation();

    const { showToast } = useShowToast();

    // Helper function to update template state with a new module
    const updateTemplateWithModule = useCallback(
        (
            templateToUpdate: PageTemplateFragment | null,
            module: PageModuleFragment,
            position: ModulePositionInput,
            isEditingModule: boolean,
        ): PageTemplateFragment | null => {
            if (!templateToUpdate) return null;

            const newTemplate = { ...templateToUpdate };
            let newRows = [...(newTemplate.properties?.rows || [])];

            // Update the existing module in-place for Optimistic UI changes
            if (isEditingModule && module.urn) {
                newRows = newRows.map((row) => ({
                    ...row,
                    modules: (row.modules || []).map((mod) => (mod.urn === module.urn ? { ...mod, ...module } : mod)),
                }));
                newTemplate.properties = {
                    ...newTemplate.properties,
                    rows: newRows,
                };
                return newTemplate;
            }

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
                    const rowModules = newRows[rowIndex]?.modules || [];
                    // Find index to insert
                    let moduleIndex: number;

                    if (position.moduleIndex !== undefined) {
                        moduleIndex = position.moduleIndex;
                    } else if (position.rowSide === 'left') {
                        moduleIndex = 0;
                    } else {
                        moduleIndex = rowModules.length;
                    }

                    // Insert module into the rows at given position
                    newRows = insertModuleIntoRows(newRows, module, { ...position, moduleIndex }, rowIndex);
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
            shouldRemoveEmptyRow: boolean,
        ): PageTemplateFragment | null => {
            if (!isValidRemovalPosition(templateToUpdate, position)) {
                return templateToUpdate;
            }

            const newTemplate = { ...templateToUpdate } as PageTemplateFragment;
            const newRows = [...(newTemplate.properties?.rows || [])];
            const { rowIndex, moduleIndex } = position;

            const row = { ...newRows[rowIndex!] };
            const originalModules = row.modules || [];

            // Remove the module using the helper function
            const updatedModules = removeModuleFromArray(originalModules, moduleUrn, moduleIndex);

            // Check if module was actually removed
            if (updatedModules.length === originalModules.length) {
                // Module not found, return original template
                return templateToUpdate;
            }

            // If the row is now empty, remove the entire row
            if (shouldRemoveEmptyRow && updatedModules.length === 0) {
                newRows.splice(rowIndex!, 1);
            } else {
                row.modules = updatedModules;
                newRows[rowIndex!] = row;
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
            currentPersonalTemplate: PageTemplateFragment | null,
        ) => {
            if (!templateToUpsert) {
                console.error('Template is required for upsert');
                return Promise.reject(new Error('Template is required for upsert'));
            }

            const isCreatingPersonalTemplate = isPersonal && !currentPersonalTemplate;

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
                    // set personal template in state after successful creation of new personal template with correct urn
                    setPersonalTemplate(data.upsertPageTemplate);
                    updateUserHomePageSettings({ variables: { input: { pageTemplate: data.upsertPageTemplate.urn } } });
                    showToast(
                        'You’ve edited your home page',
                        `To reset your home page click "Reset to Organization Default"`,
                    );
                }
            });
        },
        [upsertPageTemplateMutation, updateUserHomePageSettings, setPersonalTemplate, showToast],
    );

    const resetTemplateToDefault = () => {
        setPersonalTemplate(null);
        updateUserHomePageSettings({
            variables: {
                input: {
                    removePageTemplate: true,
                },
            },
        });
        // for now when a user resets to default, delete their personal template to prevent dangling templates
        if (personalTemplate && personalTemplate.urn !== DEFAULT_TEMPLATE_URN) {
            deletePageTemplate({ variables: { input: { urn: personalTemplate.urn } } });
        }
    };

    return {
        updateTemplateWithModule,
        removeModuleFromTemplate,
        upsertTemplate,
        resetTemplateToDefault,
    };
}
