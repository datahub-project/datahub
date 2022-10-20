import { message, Modal } from 'antd';
import React from 'react';
import { useBatchUpdateSoftDeletedMutation } from '../../../../../../../graphql/mutations.generated';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DeleteDropdown({ urns, disabled = false, refetch }: Props) {
    const [batchUpdateSoftDeletedMutation] = useBatchUpdateSoftDeletedMutation();

    const batchSoftDelete = () => {
        batchUpdateSoftDeletedMutation({
            variables: {
                input: {
                    urns,
                    deleted: true,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Deleted assets!', duration: 2 });
                    setTimeout(() => refetch?.(), 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to delete assets: \n ${e.message || ''}`, duration: 3 });
            });
    };

    return (
        <>
            <ActionDropdown
                name="Delete"
                actions={[
                    {
                        title: 'Mark as deleted',
                        onClick: () => {
                            Modal.confirm({
                                title: `Confirm Delete`,
                                content: `Are you sure you want to mark these assets as deleted? This will hide the assets
                                from future DataHub searches. If the assets are re-ingested from an external data platform, they will be restored.`,
                                onOk() {
                                    batchSoftDelete();
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
        </>
    );
}
