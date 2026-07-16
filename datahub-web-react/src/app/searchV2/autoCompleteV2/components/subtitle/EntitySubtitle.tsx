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
