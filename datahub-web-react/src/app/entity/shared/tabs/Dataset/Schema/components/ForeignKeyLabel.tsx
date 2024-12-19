import React, { useContext } from 'react';
import { Badge } from 'antd';
import styled from 'styled-components';
import { green } from '@ant-design/colors';

import { ANTD_GRAY } from '../../../../constants';
import { ForeignKeyConstraint } from '../../../../../../../types.generated';
import { FkContext } from '../utils/selectedFkContext';

const ForeignKeyBadge = styled(Badge)<{ $highlight: boolean }>`
    margin-left: 4px;
    &&& .ant-badge-count {
        background-color: ${(props) => (props.$highlight ? green[1] : ANTD_GRAY[1])};
        color: ${green[5]};
        border: 1px solid ${green[2]};
        font-size: 12px;
        font-weight: 400;
        height: 22px;
        cursor: pointer;
    }
`;

type Props = {
    highlight: boolean;
    fieldPath: string;
    constraint?: ForeignKeyConstraint | null;
    setHighlightedConstraint: (newActiveConstraint: string | null) => void;
    onClick: (params: { fieldPath: string; constraint?: ForeignKeyConstraint | null } | null) => void;
};

export default function ForeignKeyLabel({
    fieldPath,
    constraint,
    highlight,
    setHighlightedConstraint,
    onClick,
}: Props) {
    const selectedFk = useContext(FkContext);

    const onOpenFk = () => {
        if (selectedFk?.fieldPath?.trim() === fieldPath.trim() && selectedFk?.constraint?.name === constraint?.name) {
            onClick(null);
        } else {
            onClick({ fieldPath, constraint });
        }
    };

    return (
        <>
            <span
                role="button"
                tabIndex={0}
                onKeyPress={(e) => (e.key === 'Enter' ? onOpenFk() : null)}
                onClick={onOpenFk}
                onMouseEnter={() => setHighlightedConstraint(constraint?.name || null)}
                onMouseLeave={() => setHighlightedConstraint(null)}
            >
                <ForeignKeyBadge $highlight={highlight || selectedFk?.fieldPath === fieldPath} count="Foreign Key" />
            </span>
        </>
    );
}
