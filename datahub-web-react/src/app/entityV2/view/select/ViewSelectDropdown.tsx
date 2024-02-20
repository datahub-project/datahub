import React from 'react';
import styled from 'styled-components';
import { ViewSelectContent } from './ViewSelectContent';
import { ViewSelectHeader } from './ViewSelectHeader';
import { ANTD_GRAY } from '../../shared/constants';

type Props = {
    children: React.ReactNode;
    privateView: boolean;
    publicView: boolean;
    onClickCreateView: () => void;
    onClickManageViews: () => void;
    onClickViewTypeFilter: (type: string) => void;
    onChangeSearch: (text: any) => void;
};

const Container = styled.div`
    display: flex;
    width: 100%;
    padding: 0px 20px 0px 80px;
    color: ${ANTD_GRAY[1]};
    gap: 0.5rem;
    flex-direction: column;
`;

export const ViewSelectDropdown = ({
    children,
    publicView,
    privateView,
    onClickCreateView,
    onClickManageViews,
    onClickViewTypeFilter,
    onChangeSearch,
}: Props) => {
    return (
        <Container>
            <ViewSelectHeader
                onClickViewTypeFilter={onClickViewTypeFilter}
                publicView={publicView}
                privateView={privateView}
                onClickManageViews={onClickManageViews}
                onChangeSearch={onChangeSearch}
            />
            <ViewSelectContent onClickCreateView={onClickCreateView}>{children} </ViewSelectContent>
        </Container>
    );
};
