import React from 'react';
import styled from 'styled-components';
import { Image, Typography } from 'antd';
import slackLogo from '../../images/slacklogo.png';
import { toRelativeTimeString } from '../shared/time/timeUtils';
import { LinkPreview } from '../../types.generated';
import { decodeSlackMessagePreview } from './utils';
import { ANTD_GRAY } from '../entity/shared/constants';

export const AuthorImage = styled(Image)`
    width: 36px;
    height: 36px;
    border-radius: 4px;
    margin-top: 6px;
`;

export const LogoImage = styled(Image)`
    width: 18px;
    height: 18px;
    transform: translate(-55%, +160%);
    border-radius: 4px;
`;

export const MessagePreview = styled.a`
    display: flex;
    justify-content: left;
    align-items: top;
    border: 0.5px solid ${ANTD_GRAY[5]};
    padding-left: 16px;
    padding-top: 8px;
    padding-bottom: 8px;
    padding-right: 16px;
    margin-top: 4px;
    margin-bottom: 12px;
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    :hover {
        cursor: pointer;
    }
`;

export const MessageContent = styled.div`
    margin-left: 8px;
`;

export const MessageHeader = styled.div``;

export const MessageFooter = styled.div``;

interface Props {
    url: string;
    preview: LinkPreview;
}

export default function SlackMessageLinkPreview({ url, preview }: Props) {
    const messagePreview = decodeSlackMessagePreview(preview.json);

    if (messagePreview === null) {
        return null;
    }

    const { authorName, authorImageUrl, text, channelName, workspaceName, timestamp, replyCount } = messagePreview;
    return (
        <MessagePreview href={url} target="_blank" rel="noopener noreferrer">
            {authorImageUrl && (
                <>
                    <AuthorImage preview={false} src={authorImageUrl} alt="slack-author-logo" />
                    <LogoImage preview={false} src={slackLogo} alt="slack-logo" />
                </>
            )}
            <MessageContent>
                <MessageHeader>
                    <Typography.Text strong style={{ fontSize: 14 }}>
                        {authorName}
                    </Typography.Text>
                    <Typography.Text type="secondary" style={{ marginLeft: 4 }}>
                        {toRelativeTimeString(timestamp)}
                    </Typography.Text>
                </MessageHeader>
                <Typography.Paragraph
                    style={{ fontSize: 14, marginBottom: 4, padding: 0 }}
                    ellipsis={{ rows: 3, expandable: true }}
                >
                    {text}
                </Typography.Paragraph>
                <MessageFooter>
                    {(replyCount && <Typography.Text>{replyCount} replies | </Typography.Text>) || undefined}
                    <Typography.Text type="secondary">
                        #{channelName} {workspaceName && `| ${workspaceName}`}
                    </Typography.Text>
                </MessageFooter>
            </MessageContent>
        </MessagePreview>
    );
}
