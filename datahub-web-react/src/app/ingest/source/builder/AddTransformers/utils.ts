import { set, get } from 'lodash';
import YAML from 'yamljs';
import { EntityType } from '../../../../../types.generated';
import { jsonToYaml } from '../../utils';
import { Transformer, TransformerTypes } from '../types';

export function getUpdatedRecipe(displayRecipe: string, transformers: Transformer[]) {
    const jsonRecipe = YAML.parse(displayRecipe);
    const jsonTransformers = transformers
        .filter((t) => t.type)
        .map((transformer) => {
            return {
                type: transformer.type,
                config: {
                    urns: transformer.urns,
                },
            };
        });
    const transformersValue = jsonTransformers.length > 0 ? jsonTransformers : undefined;
    set(jsonRecipe, 'transformers', transformersValue);
    return jsonToYaml(JSON.stringify(jsonRecipe));
}

export function getInitialState(displayRecipe: string) {
    const jsonState = YAML.parse(displayRecipe);
    const jsonTransformers = get(jsonState, 'transformers') || [];
    return jsonTransformers.map((t) => {
        return { type: t.type, urns: t.config.urns || [] };
    });
}

export function getPlaceholderText(type: string | null) {
    let entityName = 'entities';
    if (type === TransformerTypes.ADD_OWNERS) entityName = 'owners';
    if (type === TransformerTypes.ADD_TAGS) entityName = 'tags';
    if (type === TransformerTypes.ADD_TERMS) entityName = 'glossary terms';
    if (type === TransformerTypes.ADD_DOMAIN) entityName = 'domains';
    return `Search for ${entityName}...`;
}

export function getEntityTypes(type: string | null) {
    switch (type) {
        case TransformerTypes.ADD_OWNERS:
            return [EntityType.CorpUser, EntityType.CorpGroup];
        case TransformerTypes.ADD_TAGS:
            return [EntityType.Tag];
        case TransformerTypes.ADD_TERMS:
            return [EntityType.GlossaryTerm];
        case TransformerTypes.ADD_DOMAIN:
            return [EntityType.Domain];
        default:
            return [];
    }
}
