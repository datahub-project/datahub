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
