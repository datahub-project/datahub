import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Button, toast } from '@src/alchemy-components';

import { useUnlinkPhysicalChildMutation } from '@graphql/logical.generated';

type Props = {
    childUrn: string;
    onUnlinked: () => void;
};

export default function UnlinkPhysicalChildButton({ childUrn, onUnlinked }: Props) {
    const { t } = useTranslation('logicalModels');
    const [open, setOpen] = useState(false);
    const [unlinkPhysicalChild] = useUnlinkPhysicalChildMutation();

    const onConfirm = () => {
        setOpen(false);
        unlinkPhysicalChild({ variables: { input: { childUrn } } })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('unlink.successMessage'), { duration: 2 });
                    onUnlinked();
                }
            })
            .catch((e) => toast.error(t('unlink.errorMessage', { error: e.message }), { duration: 3 }));
    };

    return (
        <>
            <Button
                variant="text"
                icon={{ icon: X }}
                onClick={() => setOpen(true)}
                data-testid="unlink-physical-child"
            />
            <ConfirmationModal
                isOpen={open}
                modalTitle={t('unlink.title')}
                modalText={t('unlink.confirm')}
                confirmButtonText={t('unlink.okText')}
                isDeleteModal
                handleConfirm={onConfirm}
                handleClose={() => setOpen(false)}
            />
        </>
    );
}
