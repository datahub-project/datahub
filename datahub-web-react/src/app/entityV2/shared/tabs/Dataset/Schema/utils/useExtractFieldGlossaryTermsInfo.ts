import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
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
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        )?.glossaryTerms;

        const uneditableTerms = defaultUneditableTerms || record?.glossaryTerms;

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
