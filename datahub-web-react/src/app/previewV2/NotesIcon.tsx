import { Popover } from '@components';
import { CampaignOutlined } from '@mui/icons-material';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Post } from '@types';

const IconWrapper = styled.div<{ count: number }>`
    display: flex;
    color: ${(props) => props.theme.colors.iconBrand};
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
    const { t } = useTranslation('entity.preview');
    return (
        <Popover content={() => <div>{t('notesCount', { count: notes.length })}</div>} placement="bottom">
            <IconWrapper count={notes.length} className={className}>
                <CampaignOutlined fontSize="inherit" />
                <Count>{notes.length}</Count>
            </IconWrapper>
        </Popover>
    );
}
