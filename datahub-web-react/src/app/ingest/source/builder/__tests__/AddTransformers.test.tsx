import { get } from 'lodash';
import YAML from 'yamljs';
import { EntityType } from '../../../../../types.generated';
import { getEntityTypes, getInitialState, getPlaceholderText, getUpdatedRecipe } from '../AddTransformers/utils';
import { TransformerTypes } from '../types';

describe('updateRecipe', () => {
    it('should get an updated recipe given transformers state', () => {
        const recipe = `\
        source: snowflake
            config:
                account_id: example_id
        `;
        const transformers = [{ type: TransformerTypes.ADD_OWNERS, urns: ['urn:li:corpuser:test'] }];
        const updatedRecipe = getUpdatedRecipe(recipe, transformers);
        const recipeObj = YAML.parse(updatedRecipe);

        expect(get(recipeObj, 'transformers')).toMatchObject([
            { type: TransformerTypes.ADD_OWNERS, config: { urns: ['urn:li:corpuser:test'] } },
        ]);
    });

    it('should get an updated recipe given transformers state where transformers already exist', () => {
        const recipe = `\
        source: snowflake
            config:
                account_id: example_id
        transformers:
            -
                type: ${TransformerTypes.ADD_OWNERS}
                config:
                    urns: 
                        - "urn:li:corpuser:jdoe"
            -
                type: ${TransformerTypes.ADD_TAGS}
                config:
                    urns: 
                        - "urn:li:tag:tag1"
        `;
        expect(get(YAML.parse(recipe), 'transformers')).toMatchObject([
            { type: TransformerTypes.ADD_OWNERS, config: { urns: ['urn:li:corpuser:jdoe'] } },
            { type: TransformerTypes.ADD_TAGS, config: { urns: ['urn:li:tag:tag1'] } },
        ]);

        const transformers = [
            { type: TransformerTypes.ADD_OWNERS, urns: ['urn:li:corpuser:test', 'urn:li:corpuser:jdoe'] },
        ];
        const updatedRecipe = getUpdatedRecipe(recipe, transformers);
        const recipeObj = YAML.parse(updatedRecipe);

        expect(get(recipeObj, 'transformers')).toMatchObject([
            { type: TransformerTypes.ADD_OWNERS, config: { urns: ['urn:li:corpuser:test', 'urn:li:corpuser:jdoe'] } },
        ]);
    });
});

describe('getInitialState', () => {
    it('should get initial state when there are transformers properly', () => {
        const recipe = `\
        source: snowflake
            config:
                account_id: example_id
        transformers:
            -
                type: ${TransformerTypes.ADD_OWNERS}
                config:
                    urns: 
                        - "urn:li:corpuser:test"
                        - "urn:li:corpuser:jdoe"
            -
                type: ${TransformerTypes.ADD_TAGS}
                config:
                    urns: 
                        - "urn:li:tag:tag1"
        `;
        const initialState = getInitialState(recipe);

        expect(initialState).toMatchObject([
            { type: TransformerTypes.ADD_OWNERS, urns: ['urn:li:corpuser:test', 'urn:li:corpuser:jdoe'] },
            { type: TransformerTypes.ADD_TAGS, urns: ['urn:li:tag:tag1'] },
        ]);
    });
});

describe('getPlaceholderText', () => {
    it('should get placeholder text for all the transformer types properly', () => {
        let placeholderText = getPlaceholderText(TransformerTypes.ADD_OWNERS);
        expect(placeholderText).toBe('Search for owners...');

        placeholderText = getPlaceholderText(TransformerTypes.ADD_DOMAIN);
        expect(placeholderText).toBe('Search for domains...');

        placeholderText = getPlaceholderText(TransformerTypes.ADD_TAGS);
        expect(placeholderText).toBe('Search for tags...');

        placeholderText = getPlaceholderText(TransformerTypes.ADD_TERMS);
        expect(placeholderText).toBe('Search for glossary terms...');
    });
});

describe('getEntityTypes', () => {
    it('should get entity types to search for given a transformer type', () => {
        let transformerType = getEntityTypes(TransformerTypes.ADD_OWNERS);
        expect(transformerType).toMatchObject([EntityType.CorpUser, EntityType.CorpGroup]);

        transformerType = getEntityTypes(TransformerTypes.ADD_DOMAIN);
        expect(transformerType).toMatchObject([EntityType.Domain]);

        transformerType = getEntityTypes(TransformerTypes.ADD_TAGS);
        expect(transformerType).toMatchObject([EntityType.Tag]);

        transformerType = getEntityTypes(TransformerTypes.ADD_TERMS);
        expect(transformerType).toMatchObject([EntityType.GlossaryTerm]);
    });
});
