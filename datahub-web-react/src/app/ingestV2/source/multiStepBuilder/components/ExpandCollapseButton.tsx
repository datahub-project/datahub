import { Button, Icon } from '@components';
import React from 'react';

interface Props {
    expanded?: boolean;
    onToggle?: () => void;
}

export function ExpandCollapseButton({ expanded, onToggle }: Props) {
    return (
        <Button variant="link" color="gray" onClick={onToggle}>
            <Icon
                source="phosphor"
                icon={expanded ? 'CaretDown' : 'CaretRight'}
                size="2xl"
                color="gray"
                colorLevel={1800}
            />
        </Button>
    );
}
