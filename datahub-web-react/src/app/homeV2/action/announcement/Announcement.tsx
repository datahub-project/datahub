/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Editor } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';

import { Post } from '@types';

const Content = styled.div`
    max-height: 368px;
    overflow: auto;
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
`;

const Text = styled.div`
    display: flex;
    justify-content: center;
    align-items: start;
    flex-direction: column;
    padding: 0px;
    &&&&& .remirror-editor.ProseMirror {
        padding: 0px;
    }
`;

const Title = styled.div`
    font-size: 20px;
    word-break: break-word;
    margin-bottom: 10px;
    font-weight: 600;
`;

const Time = styled.div`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    announcement: Post;
};

export const Announcement = ({ announcement }: Props) => {
    const timestamp = announcement?.lastModified?.time;
    return (
        <Content>
            <Text>
                <Title>{announcement?.content?.title}</Title>
                <Editor className="test-editor" content={announcement?.content?.description || ''} readOnly />
                {timestamp && <Time>{toRelativeTimeString(timestamp)}</Time>}
            </Text>
        </Content>
    );
};
