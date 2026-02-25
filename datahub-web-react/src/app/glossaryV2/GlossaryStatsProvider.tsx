import Icon from '@ant-design/icons';
import { BookmarksSimple } from '@phosphor-icons/react';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import OwnersIcon from '@images/glossary_account_circle.svg?react';
import ActiveGlossaryTermIcon from '@images/glossary_bookmark_added.svg?react';
import ApprovedGlossaryTermIcon from '@images/glossary_verified.svg?react';

const StatusWrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bgSurfaceBrand};
    border-radius: 9px;
    border: 1px solid ${(props) => props.theme.colors.bg};
    background: ${(props) => props.theme.colors.bg};
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
    color: ${(props) => props.theme.colors.text};
    font-family: Mulish;
    font-size: 20px;
    font-style: normal;
    font-weight: 700;
    line-height: normal;
    margin-left: 5px;
`;

const HeaderTitle = styled(Typography)`
    color: ${(props) => props.theme.colors.text};
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
                    <BookmarksSimple style={{ fontSize: 26 }} />
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
