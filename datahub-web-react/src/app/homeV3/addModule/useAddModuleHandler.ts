import { useCallback } from 'react';

import { AddModuleHandlerInput } from '@app/homeV3/template/types';

import { DataHubPageModuleType } from '@types';

export default function useAddModuleHandler(
    openModal: (input: AddModuleHandlerInput) => void,
    addModuleToTemplate: (input: AddModuleHandlerInput) => void,
) {
    const addModuleHandler = useCallback(
        (input: AddModuleHandlerInput) => {
            // Adding of existing module as urn is defined so we need just add it to template
            if (input.module.urn) {
                return addModuleToTemplate?.(input);
            }

            // Handling of a new module creation
            switch (input.module.type) {
                // The modules that should be created through a modal
                case DataHubPageModuleType.Hierarchy:
                case DataHubPageModuleType.AssetCollection:
                case DataHubPageModuleType.Link:
                case DataHubPageModuleType.RichText:
                    return openModal?.(input);

                // The modules that creates a new module in place
                case DataHubPageModuleType.OwnedAssets:
                case DataHubPageModuleType.Domains:
                    // TODO: add logic here to create module
                    return addModuleToTemplate?.(input);

                default:
                    console.warn('No handler to add a new module with input', input);
                    return null;
            }
        },
        [openModal, addModuleToTemplate],
    );

    return addModuleHandler;
}
