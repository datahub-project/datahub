import React from 'react';
import styled from 'styled-components';
import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Entity } from '@types';

const IconWrapper = styled.span`
    img,
    svg {
        height: 12px;
    }
`;

const DefaultIcon = styled(FolderOpenOutlined)`
    &&& {
        font-size: 14px;
    }
`;

interface Props {
    entity: Maybe<Entity>;
}

function ContextPathEntityIcon({ entity }: Props) {
    if (!entity) return null;

    // For now, we keep it simple - each parent shares the same icon within the context path.
    return (
        <IconWrapper>
            <DefaultIcon />
        </IconWrapper>
    );
}

export default ContextPathEntityIcon;
