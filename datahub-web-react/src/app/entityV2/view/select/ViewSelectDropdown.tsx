import React from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
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

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    width: 100%;
    padding: ${(props) => (props.$isShowNavBarRedesign ? '0 8px 8px 8px' : '0px 20px 0px 80px')};
    color: ${ANTD_GRAY[1]};
    gap: ${(props) => (props.$isShowNavBarRedesign ? '4px' : '0.5rem')};
    flex-direction: column;
    position: relative;
    &:hover {
        .hover-btn {
            display: flex;
        }
    }
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
    const isShowNavBarRedesign = useShowNavBarRedesign();
    return (
        <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
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
