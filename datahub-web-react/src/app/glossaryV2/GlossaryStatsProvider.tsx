import React from 'react';
import Icon from '@ant-design/icons';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import GlossaryTermIcon from '../../../src/images/glossary_collections_bookmark.svg?react';
import ActiveGlossaryTermIcon from '../../../src/images/glossary_bookmark_added.svg?react';
import ApprovedGlossaryTermIcon from '../../../src/images/glossary_verified.svg?react';
import OwnersIcon from '../../../src/images/glossary_account_circle.svg?react';

const StatusWrapper = styled.div`
    background-color: #887fae;
    border-radius: 9px;
    border: 1px solid #fff;
    background: #fff;
    margin-bottom: 10px;
    display: flex;
    justify-content: space-around;
    padding: 18px;
`;

const IconWrapper = styled.div`
    display: flex;
    margin-bottom: 5px;
`;

const CountTerms = styled.div`
    color: #171723;
    font-family: Mulish;
    font-size: 20px;
    font-style: normal;
    font-weight: 700;
    line-height: normal;
    margin-left: 5px;
`;

const HeaderTitle = styled(Typography)`
    color: #171723;
    text-align: center;
    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    font-weight: 300;
    line-height: normal;
`;

interface Props {
    totalGlossaryTerms: number;
    activeGlossaryTerms: number;
    owners: number;
    approvedGlossaryTerms: number;
}

const GlossaryStatsProvider = (props: Props) => {
    const { totalGlossaryTerms, activeGlossaryTerms, owners, approvedGlossaryTerms } = props;

    return (
        <StatusWrapper>
            <div>
                <IconWrapper>
                    <Icon style={{ fontSize: 26 }} component={GlossaryTermIcon} />
                    <CountTerms>{totalGlossaryTerms} +</CountTerms>
                </IconWrapper>
                <HeaderTitle>Total Glossary Terms</HeaderTitle>
            </div>
            <div>
                <IconWrapper>
                    <Icon style={{ fontSize: 26 }} component={ActiveGlossaryTermIcon} />
                    <CountTerms>{activeGlossaryTerms} +</CountTerms>
                </IconWrapper>
                <HeaderTitle>Active Glossary Terms</HeaderTitle>
            </div>
            <div>
                <IconWrapper>
                    <Icon style={{ fontSize: 26 }} component={OwnersIcon} />
                    <CountTerms>{owners}</CountTerms>
                </IconWrapper>
                <HeaderTitle>Owners?</HeaderTitle>
            </div>
            <div>
                <IconWrapper>
                    <Icon style={{ fontSize: 26 }} component={ApprovedGlossaryTermIcon} />
                    <CountTerms>{approvedGlossaryTerms}</CountTerms>
                </IconWrapper>
                <HeaderTitle>Approved Glossary</HeaderTitle>
            </div>
        </StatusWrapper>
    );
};

export default GlossaryStatsProvider;
