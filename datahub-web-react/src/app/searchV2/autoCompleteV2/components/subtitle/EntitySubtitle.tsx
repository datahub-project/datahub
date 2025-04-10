import React from 'react';
import { EntityType } from '@src/types.generated';
import { EntityIconProps } from '../icon/types';
import UserEntitySubtitle from './UserEntitySubtitle';
import DefaultEntitySubtitle from './DefaultEntitySubtitle';

export default function EntitySubtitle(props: EntityIconProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntitySubtitle {...props} />;
        default:
            return <DefaultEntitySubtitle {...props} />;
    }
}
