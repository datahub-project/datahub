import React from 'react';

import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';

import { MatchedField } from '@types';

export const matchedFieldPathsRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <>{downgradeV2FieldPath(matchedField.value)}</> : null;
};
