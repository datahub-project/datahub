import React from 'react';
import { EntityType } from '@src/types.generated';
import { EntityIconProps } from './types';
import UserEntityIcon from './UserEntityIcon';
import DefaultEntityIcon from './DefaultEntityIcon';

export default function EntityIcon(props: EntityIconProps) {
    const { entity } = props;

    switch (entity.type) {
        case EntityType.CorpUser:
            return <UserEntityIcon {...props} />;
        default:
            return <DefaultEntityIcon {...props} />;
    }
}
