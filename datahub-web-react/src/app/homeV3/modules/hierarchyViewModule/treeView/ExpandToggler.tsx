/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
