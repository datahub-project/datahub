import React from 'react';
import styled from 'styled-components';
import { Button, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';

import { ANTD_GRAY } from '../../../../constants';

const Container = styled.div``;

const Summary = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    box-shadow: 0px 2px 6px 0px #0000000d;
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
    &&:hover {
        color: white;
        background-color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
`;

type Props = {
    showContractBuilder: () => void;
};

export const DataContractEmptyState = ({ showContractBuilder }: Props) => {
    return (
        <Container>
            <Summary>
                <SummaryDescription>
                    <SummaryMessage>
                        <SummaryTitle level={5}>
                            No contract found
                            <div>
                                <Typography.Text type="secondary">
                                    A contract does not yet exist for this dataset
                                </Typography.Text>
                            </div>
                        </SummaryTitle>
                    </SummaryMessage>
                </SummaryDescription>
                <Actions>
                    <CreateButton onClick={showContractBuilder}>
                        <PlusOutlined />
                        CREATE
                    </CreateButton>
                </Actions>
            </Summary>
        </Container>
    );
};
