import { red } from '@ant-design/colors';
import Icon from '@ant-design/icons/lib/components/Icon';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import ExternalLink from '../../../../../images/link-out.svg?react';

const DatasetWrapper = styled.div`
    display: flex;
    align-items: center;
    margin-top: 3px;
`;

const AssertionsSummaryWrapper = styled.span`
    font-size: 10px;
    font-weight: 700;
    line-height: 13px;
    color: ${red[7]};
    border: 1px solid ${red[7]};
    border-radius: 8px;
    margin-left: 5px;
    padding: 0 3px;
    letter-spacing: 0.2px;
`;

const StyledLink = styled(Typography.Link)`
    align-items: center;
    display: flex;
    img {
        margin-right: 3px;
    }
`;

const StyledIcon = styled(Icon)`
    margin-right: 3px;
`;

interface Props {
    link: string;
    displayName: string;
    contentText: string;
}

export default function FailingEntity({ link, displayName, contentText }: Props) {
    return (
        <DatasetWrapper>
            <StyledLink href={link} target="_blank" rel="noopener noreferrer">
                <StyledIcon component={ExternalLink} />
                {displayName}
            </StyledLink>
            <AssertionsSummaryWrapper>{contentText}</AssertionsSummaryWrapper>
        </DatasetWrapper>
    );
}
