import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import LinkPhysicalChildModal from '@app/entityV2/shared/logicalModels/LinkPhysicalChildModal';
import { Button, Tooltip } from '@src/alchemy-components';

type Props = {
    logicalParentUrn: string;
    onLinked: () => void;
};

export default function AddPhysicalChildAction({ logicalParentUrn, onLinked }: Props) {
    const { t } = useTranslation('logicalModels');
    const [isOpen, setIsOpen] = useState(false);
    return (
        <>
            <Tooltip title={t('link.title')}>
                <Button
                    variant="text"
                    icon={{ icon: Plus }}
                    onClick={() => setIsOpen(true)}
                    data-testid="add-physical-child"
                />
            </Tooltip>
            {isOpen && (
                <LinkPhysicalChildModal
                    logicalParentUrn={logicalParentUrn}
                    onClose={() => setIsOpen(false)}
                    onLinked={() => {
                        setIsOpen(false);
                        onLinked();
                    }}
                />
            )}
        </>
    );
}
