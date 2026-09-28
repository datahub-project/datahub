import React from 'react';
import { useTranslation } from 'react-i18next';

import { SubTitle } from '@app/entity/shared/containers/profile/sidebar/FormInfo/components';

interface Props {
    numRemaining: number;
}

export default function RequiredPromptsRemaining({ numRemaining }: Props) {
    const { t } = useTranslation('entity.shared.containers');
    return <SubTitle addMargin>{t('formInfo.requiredQuestionsRemainingCount', { count: numRemaining })}</SubTitle>;
}
