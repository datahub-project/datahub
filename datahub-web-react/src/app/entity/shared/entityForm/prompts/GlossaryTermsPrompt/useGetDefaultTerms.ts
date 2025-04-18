import { getDefaultTermEntities, mergeObjects } from '@app/entity/shared/entityForm/prompts/GlossaryTermsPrompt/utils';
import { GenericEntityProperties } from '@app/entity/shared/types';
import useExtractFieldGlossaryTermsInfo from '@src/app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import {
    EditableSchemaMetadata,
    FormPrompt,
    GlossaryTerms,
    Maybe,
    PromptCardinality,
    SchemaField,
} from '@src/types.generated';

export const useGetDefaultTerms = (
    entityData: GenericEntityProperties | null,
    prompt: FormPrompt,
    field?: SchemaField,
    editableSchemaMetadata?: EditableSchemaMetadata | null,
) => {
    let glossaryTerms: Maybe<GlossaryTerms> | undefined;

    const extractFieldGlossaryTermsInfo = useExtractFieldGlossaryTermsInfo(editableSchemaMetadata);

    if (field) {
        if (editableSchemaMetadata) {
            const { editableTerms, uneditableTerms } = extractFieldGlossaryTermsInfo(field);
            // merge editable and uneditable terms
            glossaryTerms = mergeObjects(editableTerms, uneditableTerms);
        }
    } else glossaryTerms = entityData?.glossaryTerms;

    const existingTerms = glossaryTerms?.terms?.map((term) => term.term);
    const cardinality = prompt.glossaryTermsParams?.cardinality ?? PromptCardinality.Single;
    const allowedTerms = prompt.glossaryTermsParams?.resolvedAllowedTerms;

    return existingTerms ? getDefaultTermEntities(existingTerms, cardinality, allowedTerms) : [];
};
