/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import DefaultEntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/DefaultEntitySubtitle';
import UserEntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/UserEntitySubtitle';
import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { EntityType } from '@src/types.generated';

export default function EntitySubtitle(props: EntitySubtitleProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntitySubtitle {...props} />;
        default:
            return <DefaultEntitySubtitle {...props} />;
    }
}
