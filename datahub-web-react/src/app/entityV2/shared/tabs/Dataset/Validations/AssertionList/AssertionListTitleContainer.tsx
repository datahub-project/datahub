import { Heading, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const AssertionTitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    div {
        border-bottom: 0px;
    }
`;
const SubTitle = styled(Text).attrs({ type: 'span' })`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    line-height: inherit;
    word-break: break-word;
`;

export const AssertionListTitleContainer = () => {
    const { t } = useTranslation('entity.profile.validations');
    return (
        <AssertionTitleContainer>
            <div className="left-section">
                <Heading type="h4" size="2xl" weight="semiBold">
                    {t('assertionList.assertionsTitle')}
                </Heading>
                <SubTitle>{t('assertionList.subtitle')}</SubTitle>
            </div>
        </AssertionTitleContainer>
    );
};
