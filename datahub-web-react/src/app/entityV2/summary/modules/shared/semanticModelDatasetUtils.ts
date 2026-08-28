import { Dataset } from '@types';

export function getSemanticModelDatasetDisplayName(dataset: Dataset): string {
    return dataset.editableProperties?.name || dataset.properties?.name || dataset.name || dataset.urn;
}

export function getSemanticModelDatasetDescription(dataset: Dataset): string | undefined {
    return dataset.editableProperties?.description || dataset.properties?.description || undefined;
}

/** Prefer alias for list labels; fall back to physical/logical display name. */
export function getSemanticModelDatasetLabel(dataset: Dataset): string {
    return dataset.semanticModelProperties?.alias || getSemanticModelDatasetDisplayName(dataset);
}

/** Prefer semantic-model alias as the EntityItem title while keeping the real entity for navigation. */
export function withSemanticModelAlias(dataset: Dataset): Dataset {
    const alias = dataset.semanticModelProperties?.alias;
    if (!alias) {
        return dataset;
    }

    return {
        ...dataset,
        properties: dataset.properties
            ? { ...dataset.properties, name: alias }
            : ({ name: alias } as Dataset['properties']),
        editableProperties: dataset.editableProperties
            ? { ...dataset.editableProperties, name: undefined }
            : dataset.editableProperties,
    };
}
