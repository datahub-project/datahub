import { Card, Icon, Text, colors } from '@components';
import { MegaphoneSimple } from '@phosphor-icons/react/dist/csr/MegaphoneSimple';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import styled from 'styled-components';

import { StyledIcon } from '@app/homeV3/styledComponents';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

import { Post } from '@types';

const StyledEditor = styled(Editor)`
    border: none;

    &&& {
        .remirror-editor {
            padding: 0;
            color: ${colors.violet[500]};

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

const cardStyles = {
    backgroundColor: colors.violet[0],
    padding: '8px',
    border: 'none',
};

interface Props {
    announcement: Post;
    onDismiss: (urn: string) => void;
}

export const AnnouncementCard = ({ announcement, onDismiss }: Props) => {
    return (
        <Card
            icon={<Icon icon={MegaphoneSimple} color="violet" weight="fill" size="2xl" />}
            title={
                <Text color="violet" weight="semiBold" size="md" lineHeight="normal">
                    {announcement.content.title}
                </Text>
            }
            subTitle={
                announcement.content.description ? (
                    <StyledEditor content={announcement.content.description} readOnly />
                ) : undefined
            }
            button={
                <StyledIcon
                    icon={X}
                    color="violet"
                    size="xl"
                    onClick={() => onDismiss(announcement.urn)}
                />
            }
            width="100%"
            style={cardStyles}
            isCardClickable={false}
        />
    );
};
