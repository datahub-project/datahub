import React from 'react';

import { Assertion, Entity, Monitor } from '../../../../../../../../../types.generated';
import { AssertionSettingsLoading } from './AssertionSettingsLoading';
import { AssertionSettings } from '../../builder/details/AssertionSettings';

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
