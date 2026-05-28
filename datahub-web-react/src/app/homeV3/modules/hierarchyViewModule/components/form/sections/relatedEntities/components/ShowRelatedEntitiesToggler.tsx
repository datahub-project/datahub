import { Switch, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
`;

const LabelContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex-grow: 1;
`;

interface Props {
    isChecked: boolean;
    onChange: (isChecked: boolean) => void;
}

export default function ShowRelatedEntitiesSwitch({ isChecked, onChange }: Props) {
    const { t } = useTranslation('modules');
    return (
        <Wrapper>
            <LabelContainer>
                <Text weight="bold" lineHeight="sm">
                    {t('hierarchy.showRelatedEntitiesLabel')}
                </Text>
                <Text lineHeight="sm">{t('hierarchy.showRelatedEntitiesDescription')}</Text>
            </LabelContainer>
            <Switch label="" isChecked={isChecked} onChange={() => onChange(!isChecked)} />
        </Wrapper>
    );
}
