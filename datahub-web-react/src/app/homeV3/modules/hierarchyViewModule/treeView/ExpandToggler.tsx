import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    cursor: pointer;
`;

const NotExpandableSpaceFiller = styled.div`
    width: 16px;
`;

interface Props {
    expandable: boolean;
    expanded?: boolean;
    onToggle?: () => void;
}

export default function ExpandToggler({ expanded, expandable, onToggle }: Props) {
    if (!expandable) {
        return <NotExpandableSpaceFiller />;
    }

    return (
        <Wrapper>
            <Icon
                color="gray"
                icon="CaretRight"
                source="phosphor"
                rotate={expanded ? '90' : '0'}
                size="lg"
                onClick={onToggle}
            />
        </Wrapper>
    );
}
