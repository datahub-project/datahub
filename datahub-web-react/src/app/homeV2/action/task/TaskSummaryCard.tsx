import { ExclamationCircleOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { TaskSummaryCardLoading } from '@app/homeV2/action/task/TaskSummaryCardLoading';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 11px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 8px 16px 8px 16px;
    border: 1.5px solid ${ANTD_GRAY[4]};
    :hover {
        cursor: pointer;
        border: 1.5px solid #9884d4;
    }
    width: 100%;
    min-height: 60px;
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: center;
`;

const Alert = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
`;

const AlertIcon = styled(ExclamationCircleOutlined)`
    margin-right: 8px;
    color: #9884d4;
    font-size: 16px;
`;

const AlertContent = styled.div`
    flex: 1;
    padding-right: 8px;
    overflow: hidden;
`;

const ViewAllButton = styled.div`
    color: ${ANTD_GRAY[7]};
    padding: 2px;
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

type Props = {
    loading: boolean;
    children: React.ReactNode;
    onClick: () => void;
};

export const TaskSummaryCard = ({ loading, children, onClick }: Props) => {
    return (
        <Card onClick={onClick}>
            {(loading && <TaskSummaryCardLoading />) || (
                <Alert>
                    <AlertIcon />
                    <AlertContent>{children}</AlertContent>
                    <ViewAllButton onClick={onClick}>view</ViewAllButton>
                </Alert>
            )}
        </Card>
    );
};
