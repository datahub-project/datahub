import React from 'react';

import { downgradeV2FieldPath } from '@app/entity/dataset/profile/schema/utils/utils';

import { MatchedField } from '@types';

export const matchedFieldPathsRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <b>{downgradeV2FieldPath(matchedField.value)}</b> : null;
};
