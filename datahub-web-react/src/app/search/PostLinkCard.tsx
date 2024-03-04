import React from 'react';
import { Button, Image, Typography } from 'antd';
import { ArrowRightOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../entity/shared/constants';
import { Post } from '../../types.generated';

const CardContainer = styled(Button)`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    padding: 0px;
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
    display: flex;
    height: 100%;
    flex: 1;
    justify-content: center;
    align-items: center;
`;

const EmptyContainer = styled.div`
    padding-left: 25px;
`;

const PlatformLogo = styled(Image)`S
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const TextContainer = styled.div`
    display: flex;
    flex: 2;
`;

const FlexWrapper = styled.div<{ alignCenter?: boolean }>`
    display: flex;
    flex-direction: column;
    justify-content: center;
    flex: 2;
    ${(props) => props.alignCenter && 'align-items: center;'}
`;

const TextWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const HeaderText = styled(Typography.Text)`
    line-height: 10px;
    display: box;
    text-align: left;
    margin-bottom: 8px;
`;

const Title = styled(Typography.Title)`
    font-size: 14px;
    text-align: left;
`;

const StyledArrowOutlined = styled(ArrowRightOutlined)`
    align-self: center;
    flex: 1;
    color: black;
`;

type Props = {
    linkPost: Post;
};

export const PostLinkCard = ({ linkPost }: Props) => {
    const hasMedia = !!linkPost?.content?.media?.location;
    const link = linkPost?.content?.link || '';

    return (
        <CardContainer type="link" href={link} target="_blank" rel="noopener noreferrer">
            {hasMedia ? (
                <LogoContainer>
                    <PlatformLogo width={50} height={50} preview={false} src={linkPost?.content?.media?.location} />
                </LogoContainer>
            ) : (
                <EmptyContainer />
            )}
            <TextContainer>
                <FlexWrapper>
                    <TextWrapper>
                        <HeaderText type="secondary">Link</HeaderText>
                        <Title style={{ margin: 0 }} ellipsis={{ rows: 2 }} level={5}>
                            {linkPost?.content?.title}
                        </Title>
                    </TextWrapper>
                </FlexWrapper>
                <StyledArrowOutlined />
            </TextContainer>
        </CardContainer>
    );
};
