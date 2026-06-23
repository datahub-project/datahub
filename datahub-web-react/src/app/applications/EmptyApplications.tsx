import { Empty, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

type Props = {
    isEmptySearch: boolean;
};

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px;
`;

const StyledEmpty = styled(Empty)`
    .ant-empty-description {
        margin-bottom: 12px;
    }
`;

const EmptyApplications = ({ isEmptySearch }: Props) => {
    const { t } = useTranslation('misc');
    return (
        <EmptyContainer>
            <StyledEmpty
                description={
                    <>
                        <Typography.Text data-testid="applications-not-found">
                            {isEmptySearch ? t('applications.emptySearch') : t('applications.empty')}
                        </Typography.Text>
                        <div>
                            {!isEmptySearch && (
                                <Typography.Paragraph>{t('applications.emptyDescription')}</Typography.Paragraph>
                            )}
                        </div>
                    </>
                }
                image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        </EmptyContainer>
    );
};

export default EmptyApplications;
