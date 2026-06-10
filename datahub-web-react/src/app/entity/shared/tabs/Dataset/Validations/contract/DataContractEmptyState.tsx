import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Container = styled.div``;

const Summary = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
`;

const SummaryDescription = styled.div`
    display: flex;
    align-items: center;
`;

const SummaryMessage = styled.div`
    display: inline-block;
    margin-left: 20px;
    max-width: 350px;
`;

const SummaryTitle = styled(Typography.Title)`
    && {
        padding-bottom: 0px;
        margin-bottom: 4px;
    }
`;

const Actions = styled.div`
    margin: 12px;
    margin-right: 20px;
`;

const CreateButton = styled(Button)`
    margin-right: 12px;
    border-color: ${(props) => props.theme.styles['primary-color']};
    color: ${(props) => props.theme.styles['primary-color']};
    letter-spacing: 2px;
    text-transform: uppercase;
    &&:hover {
        color: ${(props) => props.theme.colors.textOnFillDefault};
        background-color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
`;

type Props = {
    showContractBuilder: () => void;
};

export const DataContractEmptyState = ({ showContractBuilder }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    return (
        <Container>
            <Summary>
                <SummaryDescription>
                    <SummaryMessage>
                        <SummaryTitle level={5}>
                            {t('dataContractEmptyState.title')}
                            <div>
                                <Typography.Text type="secondary">
                                    {t('dataContractEmptyState.description')}
                                </Typography.Text>
                            </div>
                        </SummaryTitle>
                    </SummaryMessage>
                </SummaryDescription>
                <Actions>
                    <CreateButton onClick={showContractBuilder}>
                        <PlusOutlined />
                        {tc('create')}
                    </CreateButton>
                </Actions>
            </Summary>
        </Container>
    );
};
