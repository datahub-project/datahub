import React from 'react';

import { MatchedField } from '../../../types.generated';
import { downgradeV2FieldPath } from '../../entityV2/dataset/profile/schema/utils/utils';

export const matchedFieldPathsRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <>{downgradeV2FieldPath(matchedField.value)}</> : null;
};
