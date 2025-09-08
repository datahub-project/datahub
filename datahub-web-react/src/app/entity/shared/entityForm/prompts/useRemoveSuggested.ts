import { useBatchRemoveTermsMutation } from '@graphql/mutations.generated';
import { FormPromptType, SubResourceType, SubmitFormPromptInput } from '@types';

export default function useRemoveSuggested(removedUrns: string[], promptType: FormPromptType, associatedUrn: string) {
    const [removeTermsMutation] = useBatchRemoveTermsMutation();

    const urnsToRemove = Array.from(new Set(removedUrns));

    const removeInitialSuggested = (submitFormInput: SubmitFormPromptInput) => {
        if (promptType === FormPromptType.GlossaryTerms) {
            if (urnsToRemove.length > 0) {
                return removeTermsMutation({
                    variables: {
                        input: {
                            termUrns: urnsToRemove,
                            resources: [
                                {
                                    resourceUrn: associatedUrn,
                                },
                            ],
                        },
                    },
                });
            }
        }

        if (promptType === FormPromptType.FieldsGlossaryTerms) {
            if (urnsToRemove.length > 0) {
                const { fieldPath } = submitFormInput;

                if (fieldPath) {
                    return removeTermsMutation({
                        variables: {
                            input: {
                                termUrns: urnsToRemove,
                                resources: [
                                    {
                                        resourceUrn: associatedUrn,
                                        subResource: fieldPath,
                                        subResourceType: SubResourceType.DatasetField,
                                    },
                                ],
                            },
                        },
                    });
                }
            }
        }

        return Promise.resolve();
    };

    return { removeInitialSuggested };
}
