/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popover } from '@components';
import { CampaignOutlined } from '@mui/icons-material';
import React from 'react';
import styled from 'styled-components';

import { pluralize } from '@app/shared/textUtil';
import { COLORS } from '@app/sharedV2/colors';

import { Post } from '@types';

const IconWrapper = styled.div<{ count: number }>`
    display: flex;
    color: ${COLORS.blue_6};
    font-size: 20px;
    line-height: 1;
`;

const Count = styled.div`
    position: relative;
    left: -1px;
    font-size: 8px;
`;

interface Props {
    notes: Post[];
    className?: string;
}

export default function NotesIcon({ notes, className }: Props) {
    return (
        <Popover content={() => <div>{`${notes.length} ${pluralize(notes.length, 'note')}`}</div>} placement="bottom">
            <IconWrapper count={notes.length} className={className}>
                <CampaignOutlined fontSize="inherit" />
                <Count>{notes.length}</Count>
            </IconWrapper>
        </Popover>
    );
}
