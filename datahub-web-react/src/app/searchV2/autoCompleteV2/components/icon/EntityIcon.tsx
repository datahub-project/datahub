import React from 'react';
<<<<<<< HEAD

import DefaultEntityIcon from '@app/searchV2/autoCompleteV2/components/icon/DefaultEntityIcon';
import UserEntityIcon from '@app/searchV2/autoCompleteV2/components/icon/UserEntityIcon';
import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import { EntityType } from '@src/types.generated';
=======
import { EntityType } from '@src/types.generated';
import { EntityIconProps } from './types';
import UserEntityIcon from './UserEntityIcon';
import DefaultEntityIcon from './DefaultEntityIcon';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function EntityIcon(props: EntityIconProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntityIcon {...props} />;
        default:
            return <DefaultEntityIcon {...props} />;
    }
}
