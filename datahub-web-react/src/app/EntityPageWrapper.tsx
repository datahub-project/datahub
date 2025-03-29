import { EntityPage } from '@app/entity/EntityPage';
import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { EntityType } from '@types';
import React from 'react';

interface Props {
    urn: string;
    entityType: EntityType;
}

export default function EntityPageWrapper(props: Props) {
    const isThemeV2 = useIsThemeV2();

    if (isThemeV2) {
        return <EntityPageV2 {...props} />;
    }
    return <EntityPage {...props} />;
}
