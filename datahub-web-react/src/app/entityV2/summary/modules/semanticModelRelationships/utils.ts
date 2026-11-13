import { ColorOptions } from '@components/theme/config';

import { Dataset, ErModelRelationshipCardinality, SemanticModelRelationship } from '@types';

export const CARDINALITY_LABEL_KEYS: Record<ErModelRelationshipCardinality, string> = {
    [ErModelRelationshipCardinality.OneOne]: 'semanticModelRelationships.cardinality.oneOne',
    [ErModelRelationshipCardinality.NOne]: 'semanticModelRelationships.cardinality.manyOne',
    [ErModelRelationshipCardinality.OneN]: 'semanticModelRelationships.cardinality.oneMany',
    [ErModelRelationshipCardinality.NN]: 'semanticModelRelationships.cardinality.manyMany',
};

export const CARDINALITY_COLORS: Record<ErModelRelationshipCardinality, ColorOptions> = {
    [ErModelRelationshipCardinality.OneOne]: 'blue',
    [ErModelRelationshipCardinality.NOne]: 'violet',
    [ErModelRelationshipCardinality.OneN]: 'green',
    [ErModelRelationshipCardinality.NN]: 'yellow',
};

export const DEFAULT_CARDINALITY_PILL_COLOR: ColorOptions = 'gray';

export function indexDatasetsByAliasOrName(datasets: Dataset[]): Map<string, Dataset> {
    const map = new Map<string, Dataset>();
    datasets.forEach((dataset) => {
        map.set(dataset.semanticModelProperties?.alias || dataset.name, dataset);
    });
    return map;
}

export function getCardinalityPillColor(cardinality?: ErModelRelationshipCardinality | null): ColorOptions {
    if (!cardinality) {
        return DEFAULT_CARDINALITY_PILL_COLOR;
    }
    return CARDINALITY_COLORS[cardinality];
}

export function getCardinalityLabelKey(cardinality: ErModelRelationshipCardinality): string {
    return CARDINALITY_LABEL_KEYS[cardinality];
}

export function getRelationshipRowKey(rel: SemanticModelRelationship, index: number): string {
    const base = rel.name ?? `${rel.from}-${rel.to}`;
    return `${base}-${index}`;
}
