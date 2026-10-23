import { Input, Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import CollapsibleSection from '@app/govern/structuredProperties/CollapsibleSection';

type Props = {
    isEditMode: boolean;
    isReadOnly: boolean;
    qualifiedName: string | undefined;
    setQualifiedName: (value: string) => void;
    error?: string;
};

const AdvancedOptions = ({ isEditMode, isReadOnly, qualifiedName, setQualifiedName, error }: Props) => {
    const { t } = useTranslation('governance.structured-properties');

    return (
        <CollapsibleSection title={t('advancedOptions.title')} dataTestId="structured-props-advanced-options">
            <Tooltip title={isEditMode && t('advancedOptions.qualifiedNameDisabledTooltip')} showArrow={false}>
                <div>
                    <Input
                        label={t('advancedOptions.qualifiedName')}
                        helperText={t('advancedOptions.qualifiedNameTooltip')}
                        placeholder={t('advancedOptions.qualifiedNamePlaceholder')}
                        value={qualifiedName ?? ''}
                        setValue={setQualifiedName}
                        error={error}
                        isDisabled={isEditMode || isReadOnly}
                    />
                </div>
            </Tooltip>
        </CollapsibleSection>
    );
};

export default AdvancedOptions;
