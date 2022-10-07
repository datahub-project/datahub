import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../entity/shared/constants';
import { Post } from '../../types.generated';

const CardContainer = styled.div`
    display: flex;
    flex-direction: row;
    margin-right: 12px;
    margin-left: 12px;
    margin-bottom: 12px;
    height: 140px;
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
    white-space: unset;
`;

const TextContainer = styled.div`
    margin-left: 12px;
    display: flex;
    justify-content: center;
    align-items: start;
    flex-direction: column;
`;

const Title = styled(Typography.Title)`
    word-break: break-word;
`;

const HeaderText = styled(Typography.Text)`
    margin-top: 12px;
`;

const AnnouncementText = styled(Typography.Paragraph)`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    textPost: Post;
};

export const PostTextCard = ({ textPost }: Props) => {
    return (
        <CardContainer>
            <TextContainer>
                <HeaderText type="secondary">Announcement</HeaderText>
                <Title
                    ellipsis={{
                        rows: 1,
                    }}
                    level={5}
                >
                    {textPost?.content?.title}
                </Title>
                <AnnouncementText>{textPost?.content?.description}</AnnouncementText>
            </TextContainer>
        </CardContainer>
    );
};
