import { Button, Icon } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';

interface Props {
    expanded?: boolean;
    onToggle?: () => void;
}

export function ExpandCollapseButton({ expanded, onToggle }: Props) {
    return (
        <Button variant="link" color="gray" onClick={onToggle} data-testid="expand-collapse-button">
            <Icon icon={expanded ? CaretDown : CaretRight} size="2xl" color="gray" colorLevel={1800} />
        </Button>
    );
}
