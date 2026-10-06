import { Table, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DatasetAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionDescription';
import { FieldAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/FieldAssertionDescription';
import { SqlAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/SqlAssertionDescription';
import { VolumeAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/VolumeAssertionDescription';
import {
    getCustomAssertionFields,
    hasStructuredAssertionDescriptionFields,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/shared/structuredAssertionUtils';
import { DataContractAssertionStatus } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/DataContractAssertionStatus';
import { DataContractSummaryFooter } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/DataContractSummaryFooter';

import { Assertion, AssertionType, DataQualityContract } from '@types';

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
`;

type Props = {
    contracts: DataQualityContract[];
    showAction?: boolean;
};

export const DataQualityContractSummary = ({ contracts, showAction = false }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tl } = useTranslation('common.labels');
    const assertions: Assertion[] = contracts?.map((contract) => contract.assertion);

    const columns = [
        {
            title: () => <ColumnHeader>{t('contractColumn.assertion')}</ColumnHeader>,
            render: (assertion: Assertion) => (
                <>
                    {assertion.info?.type === AssertionType.Dataset && assertion.info?.datasetAssertion && (
                        <DatasetAssertionDescription
                            scope={assertion.info.datasetAssertion.scope}
                            aggregation={assertion.info.datasetAssertion.aggregation}
                            operator={assertion.info.datasetAssertion.operator}
                            fields={assertion.info.datasetAssertion.fields}
                            parameters={assertion.info.datasetAssertion.parameters}
                            nativeType={assertion.info.datasetAssertion.nativeType}
                            nativeParameters={assertion.info.datasetAssertion.nativeParameters}
                            logic={assertion.info.datasetAssertion.logic}
                        />
                    )}
                    {assertion.info?.volumeAssertion && (
                        <VolumeAssertionDescription assertionInfo={assertion.info?.volumeAssertion} />
                    )}
                    {assertion.info?.fieldAssertion && (
                        <FieldAssertionDescription assertionInfo={assertion.info?.fieldAssertion} />
                    )}
                    {assertion.info?.sqlAssertion && <SqlAssertionDescription assertionInfo={assertion.info} />}
                    {assertion.info?.type === AssertionType.Custom &&
                        (() => {
                            // Prefer explicit description — same as Quality list / profile primary label.
                            if (assertion.info?.description) {
                                return <Typography.Text>{assertion.info.description}</Typography.Text>;
                            }
                            const custom = assertion.info?.customAssertion;
                            if (
                                custom &&
                                hasStructuredAssertionDescriptionFields({
                                    scope: custom.scope,
                                    operator: custom.operator,
                                    aggregation: custom.aggregation,
                                    nativeType: custom.nativeType,
                                })
                            ) {
                                return (
                                    <DatasetAssertionDescription
                                        scope={custom.scope}
                                        aggregation={custom.aggregation}
                                        operator={custom.operator}
                                        fields={getCustomAssertionFields(custom)}
                                        parameters={custom.parameters}
                                        nativeType={custom.nativeType}
                                        nativeParameters={custom.nativeParameters}
                                        logic={custom.logic}
                                    />
                                );
                            }
                            return <Typography.Text>{assertion.info?.customAssertion?.type}</Typography.Text>;
                        })()}
                </>
            ),
        },
        {
            title: () => (
                <ColumnHeader style={{ display: 'flex', justifyContent: 'center' }}>{tl('status')}</ColumnHeader>
            ),
            render: (assertion: Assertion) => <DataContractAssertionStatus assertion={assertion} />,
        },
    ];

    const data = (assertions || []).map((assertion) => ({
        ...assertion,
        key: assertion.urn,
    }));

    return (
        <Container>
            <TitleText>{t('contractSection.dataQuality')}</TitleText>
            <SummaryContainer>
                <StyledTable
                    pagination={false}
                    columns={columns}
                    dataSource={data}
                    footer={() => (
                        <DataContractSummaryFooter
                            assertions={assertions}
                            passingText={t('contractStatus.passingText.dataQuality')}
                            failingText={t('contractStatus.failingText.dataQuality')}
                            errorText={t('contractStatus.errorText.dataQuality')}
                            actionText={t('contractStatus.action.viewDataQuality')}
                            showAction={showAction}
                        />
                    )}
                />
            </SummaryContainer>
        </Container>
    );
};
