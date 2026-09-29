import { Button } from '@components';
import { ArrowsOutSimple } from '@phosphor-icons/react/dist/csr/ArrowsOutSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import CopyQuery from '@app/entityV2/shared/tabs/Dataset/Queries/CopyQuery';

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: right;
`;

const Actions = styled.div<{ opacity?: number }>`
    padding: 0px;
    height: 0px;
    transform: translate(-12px, 12px);
    opacity: ${(props) => props.opacity || 1.0};
`;

const ExpandButton = styled(Button)`
    margin-left: 8px;
`;

type Props = {
    query: string;
    focused: boolean;
    onClickExpand?: (newQuery) => void;
};

export default function QueryCardHeader({ query, focused, onClickExpand }: Props) {
    const { t: tc } = useTranslation('common.actions');

    return (
        <Header>
            <Actions opacity={(!focused && 0.3) || 1.0}>
                <CopyQuery query={query} />
                <ExpandButton
                    variant="outline"
                    color="gray"
                    size="sm"
                    isCircle
                    icon={{ icon: ArrowsOutSimple }}
                    onClick={onClickExpand}
                    aria-label={tc('expand')}
                />
            </Actions>
        </Header>
    );
}
