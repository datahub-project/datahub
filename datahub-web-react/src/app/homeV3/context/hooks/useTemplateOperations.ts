import { useCallback } from 'react';

import { AddModuleHandlerInput } from '@app/homeV3/template/types';

import {
    PageModuleFragment,
    PageTemplateFragment,
    useUpsertPageTemplateMutation,
} from '@graphql/template.generated';
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
            position: AddModuleHandlerInput,
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
                const rowIndex = position.rowIndex;
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

    // Helper function to upsert template
    const upsertTemplate = useCallback(
        (templateToUpsert: PageTemplateFragment | null, isPersonal: boolean, personalTemplate: PageTemplateFragment | null) => {
            if (!templateToUpsert) {
                throw new Error('Template is required for upsert');
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
        upsertTemplate,
    };
} 