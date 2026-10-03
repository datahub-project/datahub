import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ForeignKeyPill } from '@app/entityV2/shared/tabs/Dataset/Schema/components/ConstraintLabels';

// The label shares its appearance with the other constraint labels and only adds what a button needs,
// so a change to the shared pill keeps the two in step.
const ForeignKeyPillButton = styled(ForeignKeyPill)`
    cursor: pointer;
    line-height: inherit;
`;

interface Props {
    onClick: () => void;
}

export default function ClickableForeignKeyLabel({ onClick }: Props) {
    const { t } = useTranslation('entity.profile.schema');

    return (
        <ForeignKeyPillButton
            as="button"
            type="button"
            onClick={(event) => {
                event.stopPropagation();
                onClick();
            }}
        >
            {t('constraintLabels.foreignKey')}
        </ForeignKeyPillButton>
    );
}
