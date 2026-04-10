import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import React, { useCallback } from 'react';

import { Icon } from '@components/components/Icon';

interface Props {
    isExpanded?: boolean;
    onClick?: () => void;
}

export function ExpandCollapseButton({ isExpanded, onClick }: Props) {
    const onClickHandler = useCallback(
        (e: React.MouseEvent) => {
            e.stopPropagation();
            e.preventDefault();
            onClick?.();
        },
        [onClick],
    );

    return (
        <Icon
            onClick={onClickHandler}
            icon={CaretLeft}
            rotate={isExpanded ? '90' : '270'}
            size="xl"
            color="gray"
            style={{ cursor: 'pointer', marginLeft: '4px' }}
        />
    );
}
