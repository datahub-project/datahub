import React from 'react';

import { MatchedField } from '../../../types.generated';
import { downgradeV2FieldPath } from '../../entity/dataset/profile/schema/utils/utils';

export const matchedFieldPathsRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <b>{downgradeV2FieldPath(matchedField.value)}</b> : null;
};
