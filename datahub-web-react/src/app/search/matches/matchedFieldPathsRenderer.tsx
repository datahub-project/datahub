/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { downgradeV2FieldPath } from '@app/entity/dataset/profile/schema/utils/utils';

import { MatchedField } from '@types';

export const matchedFieldPathsRenderer = (matchedField: MatchedField) => {
    return matchedField?.name === 'fieldPaths' ? <b>{downgradeV2FieldPath(matchedField.value)}</b> : null;
};
