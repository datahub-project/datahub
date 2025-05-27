import { message } from 'antd';
import React, { useState } from 'react';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { SetDomainModal } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SetDomainModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useBatchSetDomainMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DomainsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [isUnsetModalVisible, setIsUnsetModalVisible] = useState(false);

    const [batchSetDomainMutation] = useBatchSetDomainMutation();

    const batchUnsetDomains = () => {
        batchSetDomainMutation({
            variables: {
                input: {
                    resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Removed Domain!', duration: 2 });
                    refetch?.();
                }
                setIsUnsetModalVisible(false);
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to remove assets from Domain: \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name="Domain"
                actions={[
                    {
                        title: 'Set Domain',
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Unset Domain',
                        onClick: () => {
                            setIsUnsetModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <SetDomainModal
                    urns={urns}
                    onCloseModal={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                />
            )}
            <ConfirmationModal
                isOpen={isUnsetModalVisible}
                handleClose={() => setIsUnsetModalVisible(false)}
                handleConfirm={batchUnsetDomains}
                modalTitle="Domain will be removed for the selected assets"
                modalText="Are you sure you want to unset Domain for these assets?"
            />
        </>
    );
}
