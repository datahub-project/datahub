import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { HealthSummaryCardLoadingSection } from './HealthSummaryCardLoading';

const Card = styled.div<{ clickable: boolean }>`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 11px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 8px 16px 8px 16px;
    border: 1.5px solid ${ANTD_GRAY[4]};
    :hover {
        ${(props) =>
            props.clickable &&
            `cursor: pointer;
        border: 1.5px solid #d07bb3;`}
    }
    width: 100%;
`;

type Props = {
    loading: boolean;
    children: React.ReactNode;
    onClick?: () => void;
};

export const HealthSummaryCard = ({ loading, children, onClick }: Props) => {
    return (
        <Card onClick={onClick} clickable={onClick !== undefined}>
            {(loading && <HealthSummaryCardLoadingSection />) || { children }}
        </Card>
    );
};
