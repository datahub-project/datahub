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

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';

import { Post } from '@types';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 8px;
    background-color: #ffffff;
    overflow: auto;
`;

const Text = styled.div`
    display: flex;
    justify-content: center;
    align-items: start;
    flex-direction: column;
    padding: 20px;
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

export const AnnouncementCard = ({ announcement }: Props) => {
    const timestamp = announcement?.lastModified?.time;
    return (
        <Card>
            <Text>
                <Title>{announcement?.content?.title}</Title>
                <Editor className="test-editor" content={announcement?.content?.description || ''} readOnly />
                {timestamp && <Time>{toRelativeTimeString(timestamp)}</Time>}
            </Text>
        </Card>
    );
};
