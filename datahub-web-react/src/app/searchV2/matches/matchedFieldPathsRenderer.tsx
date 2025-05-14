import React from 'react';

import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';

import { MatchedField } from '@types';

export const matchedFieldPathsRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <>{downgradeV2FieldPath(matchedField.value)}</> : null;
};
