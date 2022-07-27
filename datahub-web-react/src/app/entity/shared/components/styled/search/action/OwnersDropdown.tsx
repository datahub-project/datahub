import React, { useState } from 'react';
import { EntityType } from '../../../../../../../types.generated';
import { AddOwnersModal } from '../../../../containers/profile/sidebar/Ownership/AddOwnersModal';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

// eslint-disable-next-line
export default function OwnersDropdown({ urns, disabled = false }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);
    return (
        <>
            <ActionDropdown
                name="Owners"
                actions={[
                    {
                        title: 'Add owners',
                        onClick: () => setShowAddModal(true),
                    },
                    {
                        title: 'Remove owners',
                        onClick: () => null,
                    },
                ]}
                disabled={disabled}
            />
            {showAddModal && urns.length > 0 && (
                <AddOwnersModal
                    urn={urns[0]}
                    type={EntityType.CorpUser}
                    onCloseModal={() => {
                        setShowAddModal(false);
                    }}
                />
            )}
        </>
    );
}
