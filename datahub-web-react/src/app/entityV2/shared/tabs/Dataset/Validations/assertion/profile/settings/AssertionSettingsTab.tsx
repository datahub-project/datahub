import React from 'react';

import { AssertionSettings } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/AssertionSettings';
import { AssertionSettingsLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/settings/AssertionSettingsLoading';

import { Assertion, Entity, Monitor } from '@types';

type Props = {
    loading: boolean;
    assertion?: Assertion;
    entity: Entity;
    monitor?: Monitor;
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
            {loading ? (
                <AssertionSettingsLoading />
            ) : (
                <AssertionSettings
                    assertion={assertion as Assertion}
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
