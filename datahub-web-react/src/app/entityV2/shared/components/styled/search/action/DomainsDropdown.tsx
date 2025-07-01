import { Modal, message } from 'antd';
import React, { useState } from 'react';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { SetDomainModal } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SetDomainModal';
import { handleBatchError } from '@app/entityV2/shared/utils';

import { useBatchSetDomainMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DomainsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
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
                            Modal.confirm({
                                title: `If you continue, Domain will be removed for the selected assets.`,
                                content: `Are you sure you want to unset Domain for these assets?`,
                                onOk() {
                                    batchUnsetDomains();
                                },
                                onCancel() {},
                                okText: 'Yes',
                                maskClosable: true,
                                closable: true,
                            });
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
        </>
    );
}
