/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { green } from '@ant-design/colors';
import { Badge } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { FkContext } from '@app/entity/shared/tabs/Dataset/Schema/utils/selectedFkContext';

import { ForeignKeyConstraint } from '@types';

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
