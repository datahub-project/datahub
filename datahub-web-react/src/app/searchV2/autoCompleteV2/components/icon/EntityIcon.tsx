/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import DefaultEntityIcon from '@app/searchV2/autoCompleteV2/components/icon/DefaultEntityIcon';
import UserEntityIcon from '@app/searchV2/autoCompleteV2/components/icon/UserEntityIcon';
import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import { EntityType } from '@src/types.generated';

export default function EntityIcon(props: EntityIconProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntityIcon {...props} />;
        default:
            return <DefaultEntityIcon {...props} />;
    }
}
