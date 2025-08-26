import { TestBuilderState } from '@app/tests/builder/types';

import { Test, UpdateTestInput } from '@types';

export function generateEditTestInput(input: TestBuilderState | Test): UpdateTestInput {
    return {
        name: input.name || '',
        category: input.category || '',
        description: input.description,
        definition: { json: input.definition?.json },
        status: input.status && input.status.mode ? { mode: input.status.mode } : null,
    };
}
