import React from 'react';

import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import DefaultEntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/DefaultEntitySubtitle';
import UserEntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/UserEntitySubtitle';
import { EntityType } from '@src/types.generated';

export default function EntitySubtitle(props: EntityIconProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntitySubtitle {...props} />;
        default:
            return <DefaultEntitySubtitle {...props} />;
    }
}
