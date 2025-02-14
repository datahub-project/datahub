import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../entityV2/shared/constants';
import { Post } from '../../../../types.generated';
import { Editor } from '../../../entityV2/shared/tabs/Documentation/components/editor/Editor';
import { toRelativeTimeString } from '../../../shared/time/timeUtils';

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
