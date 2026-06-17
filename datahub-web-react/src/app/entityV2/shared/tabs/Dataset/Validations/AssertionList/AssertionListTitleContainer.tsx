import { Typography } from 'antd';
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
const AssertionListTitle = styled(Typography.Title)`
    && {
        margin-bottom: 0px;
    }
`;

const SubTitle = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export const AssertionListTitleContainer = () => {
    const { t } = useTranslation('entity.profile.validations');
    return (
        <AssertionTitleContainer>
            <div className="left-section">
                <AssertionListTitle level={4}>{t('assertionList.assertionsTitle')}</AssertionListTitle>
                <SubTitle>{t('assertionList.subtitle')}</SubTitle>
            </div>
        </AssertionTitleContainer>
    );
};
