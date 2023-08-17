import React from 'react';

import { MatchedField } from '../../../types.generated';
import { downgradeV2FieldPath } from './profile/schema/utils/utils';

export const datasetMatchedFieldRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <b>{downgradeV2FieldPath(matchedField.value)}</b> : null;
};
