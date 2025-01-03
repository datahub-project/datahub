import { FormForActor, FormState } from '@src/types.generated';

export function filterFormsForUser(formForActor: FormForActor) {
    return (
        (formForActor.numEntitiesToComplete || 0) > 0 &&
        formForActor.form.info.status.state === FormState.Published &&
        formForActor.form.exists
    );
}
