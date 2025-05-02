import { pathMatchesNewPath } from '@src/app/entity/dataset/profile/schema/utils/utils';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { getProposedItemsByType } from '@src/app/entityV2/shared/utils';
import { findFieldPathProposal } from '@src/app/shared/tags/utils/proposalUtils';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import {
    ActionRequest,
    ActionRequestType,
    EditableSchemaMetadata,
    GlossaryTerms,
    SchemaField,
} from '@src/types.generated';

export default function useExtractFieldGlossaryTermsInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    return (record: SchemaField, defaultUneditableTerms: GlossaryTerms | null = null) => {
        const editableTerms = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.glossaryTerms;

        const businessAttributeTerms =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                ?.glossaryTerms?.terms || [];
        const uneditableTerms = {
            terms: [
                ...(defaultUneditableTerms?.terms || record?.glossaryTerms?.terms || []),
                ...businessAttributeTerms,
            ],
        };

        const proposedTerms: ActionRequest[] = findFieldPathProposal(
            getProposedItemsByType(
                (baseEntity?.dataset?.proposals || []) as ActionRequest[],
                ActionRequestType.TermAssociation,
            ) || [],
            record.fieldPath,
        );

        const numberOfTerms =
            (editableTerms?.terms?.length ?? 0) + (uneditableTerms?.terms?.length ?? 0) + proposedTerms.length;

        return { editableTerms, uneditableTerms, proposedTerms, numberOfTerms };
    };
}
