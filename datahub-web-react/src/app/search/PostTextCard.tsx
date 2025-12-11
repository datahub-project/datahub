/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';

import { Post } from '@types';

const CardContainer = styled.div`
    display: flex;
    flex-direction: row;
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
    white-space: unset;
    padding-bottom: 4px;
`;

const TextContainer = styled.div`
    margin-left: 12px;
    display: flex;
    justify-content: center;
    align-items: start;
    flex-direction: column;
`;

const TitleText = styled(Typography.Title)`
    word-break: break-word;
    min-height: 20px;
    &&& {
        margin-top: 8px;
    }
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
                <TitleText
                    ellipsis={{
                        rows: 1,
                    }}
                    level={5}
                >
                    {textPost?.content?.title}
                </TitleText>
                <AnnouncementText>{textPost?.content?.description}</AnnouncementText>
            </TextContainer>
        </CardContainer>
    );
};
