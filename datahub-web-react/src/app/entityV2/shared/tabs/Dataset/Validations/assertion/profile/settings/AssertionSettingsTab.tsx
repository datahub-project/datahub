import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { AssertionSettings } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/AssertionSettings';
import { AssertionSettingsLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/settings/AssertionSettingsLoading';

import { Assertion, Entity, Maybe, Monitor } from '@types';

type Props = {
    loading: boolean;
    assertion: Maybe<Assertion>;
    entity?: Entity | GenericEntityProperties;
    monitor?: Maybe<Monitor>;
    editable?: boolean;
    editAllowed?: boolean;
    refetch: () => void;
};

export const AssertionSettingsTab = ({
    loading,
    assertion,
    entity,
    monitor,
    editable,
    editAllowed,
    refetch,
}: Props) => {
    return (
        <>
            {loading || !assertion || !entity ? (
                <AssertionSettingsLoading />
            ) : (
                <AssertionSettings
                    assertion={assertion}
                    entity={entity}
                    refetch={refetch}
                    monitor={monitor}
                    editable={editable}
                    editAllowed={editAllowed}
                />
            )}
        </>
    );
};
