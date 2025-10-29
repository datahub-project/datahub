import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components/macro';

import PostItemMenu from '@app/settings/posts/PostItemMenu';

export interface PostEntry {
    urn: string;
    title: string;
    contentType: string;
    description?: Maybe<string>;
    link?: string | null;
    imageUrl?: string;
}

const PostText = styled.div<{ minWidth?: number }>`
    ${(props) => props.minWidth !== undefined && `min-width: ${props.minWidth}px;`}
`;
