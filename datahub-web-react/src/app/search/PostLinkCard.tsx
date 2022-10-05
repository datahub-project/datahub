import React from 'react';
// import { Link } from 'react-router-dom';
import { Button, Image, Typography } from 'antd';
import { ArrowRightOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../entity/shared/constants';
import { Post } from '../../types.generated';

const CardContainer = styled(Button)<{ isLastCardInRow?: boolean }>`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    margin-right: ${(props) => (props.isLastCardInRow ? '0%' : '4%')};
    margin-left: 12px;
    margin-bottom: 12px;
    width: 29%;
    height: 100px;
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
    white-space: unset;
`;

const LogoContainer = styled.div`
    margin-top: 25px;
    margin-left: 25px;
    margin-right: 40px;
`;

const PlatformLogo = styled(Image)`
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const TextContainer = styled.div`
    display: flex;
    flex: 1;
    justify-content: center;
    align-items: start;
    flex-direction: column;
`;

const HeaderText = styled(Typography.Text)`
    line-height: 10px;
    margin-top: 12px;
`;

const TitleDiv = styled.div`
    display: flex;
    justify-content: space-evenly;
    align-items: center;
    gap: 6px;
    font-size: 14px;
`;

const Title = styled(Typography.Title)`
    word-break: break-word;
`;

const NUM_CARDS_PER_ROW = 3;

type Props = {
    linkPost: Post;
    index: number;
};

export const PostLinkCard = ({ linkPost, index }: Props) => {
    const hasMedia = !!linkPost?.content?.media?.location;
    const link = linkPost?.content?.link || '';
    const isLastCardInRow = (index + 1) % NUM_CARDS_PER_ROW === 0;

    return (
        <CardContainer type="link" href={link} isLastCardInRow={isLastCardInRow}>
            {hasMedia && (
                <LogoContainer>
                    <PlatformLogo width={50} height={50} preview={false} src={linkPost?.content?.media?.location} />
                </LogoContainer>
            )}
            <TextContainer>
                <HeaderText type="secondary">Link</HeaderText>
                <Title level={5}>
                    <TitleDiv>
                        {linkPost?.content?.title}
                        <ArrowRightOutlined />
                    </TitleDiv>
                </Title>
            </TextContainer>
        </CardContainer>
    );
};
