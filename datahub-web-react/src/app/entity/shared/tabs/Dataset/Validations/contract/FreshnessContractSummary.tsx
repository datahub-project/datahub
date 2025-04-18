import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';
import { FreshnessContract } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { DataContractSummaryFooter } from './DataContractSummaryFooter';
import { FreshnessScheduleSummary } from './FreshnessScheduleSummary';

const Container = styled.div`
    padding: 28px;
`;

const TitleText = styled.div`
    color: ${ANTD_GRAY[7]};
    margin-bottom: 20px;
    letter-spacing: 1px;
`;

const ThinDivider = styled(Divider)`
    && {
        padding: 0px;
        margin: 0px;
    }
`;

const Header = styled.div`
    color: ${ANTD_GRAY[8]};
    letter-spacing; 4px;
    padding-top: 8px;
    padding: 12px;
    background-color: ${ANTD_GRAY[2]};
`;

const Body = styled.div`
    padding: 12px;
`;

const Footer = styled.div`
    padding-top: 8px;
    padding: 12px;
    background-color: ${ANTD_GRAY[2]};
`;

const SummaryContainer = styled.div`
    width: 100%;
    border-radius: 8px;
    box-shadow: 0px 0px 4px rgba(0, 0, 0, 0.1);
`;

type Props = {
    contracts: FreshnessContract[];
    showAction?: boolean;
};

export const FreshnessContractSummary = ({ contracts, showAction = false }: Props) => {
    // TODO: Support multiple per-asset contracts.
    const firstContract = (contracts.length && contracts[0]) || undefined;
    const assertionDefinition = firstContract?.assertion?.info?.freshnessAssertion?.schedule;
    const evaluationSchedule = (firstContract?.assertion as any)?.monitor?.relationships[0]?.entity?.info
        ?.assertionMonitor?.assertions[0]?.schedule;

    return (
        <Container>
            <TitleText>FRESHNESS</TitleText>
            <SummaryContainer>
                <Header>
                    <ClockCircleOutlined style={{ marginRight: 8 }} />
                    UPDATE FREQUENCY
                </Header>
                <Body>
                    {!assertionDefinition && <>No contract found :(</>}
                    <b>
                        {assertionDefinition && (
                            <FreshnessScheduleSummary
                                definition={assertionDefinition}
                                evaluationSchedule={evaluationSchedule}
                            />
                        )}
                    </b>
                </Body>
                <ThinDivider />
                <Footer>
                    <DataContractSummaryFooter
                        assertions={(firstContract && [firstContract?.assertion]) || []}
                        passingText="Meeting freshness contract"
                        failingText="Violating freshness contract"
                        errorText="Freshness contract assertions are completing with errors"
                        actionText="view freshness assertions"
                        showAction={showAction}
                    />
                </Footer>
            </SummaryContainer>
        </Container>
    );
};
