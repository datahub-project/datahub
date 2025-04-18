import React from 'react';
<<<<<<< HEAD

import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import DefaultEntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/DefaultEntitySubtitle';
import UserEntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/UserEntitySubtitle';
import { EntityType } from '@src/types.generated';
=======
import { EntityType } from '@src/types.generated';
import { EntityIconProps } from '../icon/types';
import UserEntitySubtitle from './UserEntitySubtitle';
import DefaultEntitySubtitle from './DefaultEntitySubtitle';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function EntitySubtitle(props: EntityIconProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntitySubtitle {...props} />;
        default:
            return <DefaultEntitySubtitle {...props} />;
    }
}
