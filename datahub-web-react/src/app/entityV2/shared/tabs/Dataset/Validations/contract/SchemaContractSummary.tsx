import { Table } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DataContractSummaryFooter } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/DataContractSummaryFooter';

import { SchemaContract } from '@types';

const TitleText = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    margin-bottom: 20px;
    letter-spacing: 1px;
`;

const ColumnHeader = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Container = styled.div`
    padding: 28px;
`;

const SummaryContainer = styled.div`
    width: 100%;
    display: flex;
    align-items: center;
`;

const StyledTable = styled(Table)`
    width: 100%;
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    height: 100%;
`;

type Props = {
    contracts: SchemaContract[];
    showAction?: boolean;
};

export const SchemaContractSummary = ({ contracts, showAction = false }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tl } = useTranslation('common.labels');
    const firstContract = (contracts.length && contracts[0]) || undefined;
    const schemaMetadata = firstContract?.assertion?.info?.schemaAssertion?.schema;

    const columns = [
        {
            title: () => <ColumnHeader>{tl('name')}</ColumnHeader>,
            render: (field) => <>{field.fieldPath}</>,
        },
        {
            title: () => <ColumnHeader>{t('schemaSummary.typeColumn')}</ColumnHeader>,
            render: (field) => <>{field.type}</>,
        },
    ];

    const data = (schemaMetadata?.fields || []).map((field) => ({
        ...field,
        key: field.fieldPath,
    }));

    return (
        <Container>
            <TitleText>{t('contractSection.schema')}</TitleText>
            <SummaryContainer>
                <StyledTable
                    pagination={false}
                    columns={columns}
                    dataSource={data}
                    footer={() => (
                        <DataContractSummaryFooter
                            assertions={(firstContract && [firstContract?.assertion]) || []}
                            passingText={t('contractStatus.passingText.schema')}
                            failingText={t('contractStatus.failingText.schema')}
                            errorText={t('contractStatus.errorText.schema')}
                            actionText={t('contractStatus.action.viewSchema')}
                            showAction={showAction}
                        />
                    )}
                />
            </SummaryContainer>
        </Container>
    );
};
