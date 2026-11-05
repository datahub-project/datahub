import React from 'react';
import styled from 'styled-components';

import { Editor } from '@app/sharedV2/editor/Editor';
import { Alert } from '@src/alchemy-components';

import { Post } from '@types';

const DescriptionEditor = styled(Editor)`
    border: none;

    &&& {
        .remirror-editor {
            padding: 0;
            color: inherit !important;

            p {
                margin-bottom: 0;
            }

            h1,
            h2,
            h3,
            h4,
            h5,
            h6 {
                color: inherit;
            }
        }
    }
`;

interface Props {
    announcement: Post;
    onDismiss: (urn: string) => void;
}

export const AnnouncementCard = ({ announcement, onDismiss }: Props) => {
    return (
        <Alert
            variant="brand"
            title={announcement.content.title}
            description={
                announcement.content.description ? (
                    <DescriptionEditor content={announcement.content.description} readOnly />
                ) : undefined
            }
            onClose={() => onDismiss(announcement.urn)}
        />
    );
};
