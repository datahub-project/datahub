import { Drawer } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../../../EntityContext';
import { useGetTimelineQuery } from '../../../../../../../graphql/timeline.generated';
import { ChangeCategoryType } from '../../../../../../../types.generated';
import ChangeTransaction from './ChangeTransaction';

const StyledDrawer = styled(Drawer)`
    &&& .ant-drawer-body {
        padding: 0;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        height: 100%;
    }

    &&& .ant-drawer-content-wrapper {
        box-shadow: -20px 0px 44px 0px rgba(0, 0, 0, 0.15);
    }
`;

const DrawerContent = styled.div`
    height: 100%;
`;

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    background: #113633;
    color: #fff;
    font-size: 14px;
    font-weight: 600;
`;

const TimelineTabSubheader = styled.div`
    display: flex;
    justify-content: right;
    padding: 4px 16px;
    background: #f6f7fa;
`;

const ChangeTransactionList = styled.div`
    display: flex;
    flex-direction: column;
    overflow-y: scroll;
    padding: 26px;
`;

type Props = {
    open: boolean;
    onClose: () => void;
};

const HistorySidebar = ({ open, onClose }: Props) => {
    const { urn } = useEntityData();

    const timelineResult = useGetTimelineQuery({
        // also pass in the changeCategories
        variables: {
            input: {
                urn,
                changeCategories: [ChangeCategoryType.TechnicalSchema, ChangeCategoryType.Documentation],
            },
        },
    });

    return (
        <StyledDrawer
            open={open}
            onClose={() => onClose()}
            getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
            contentWrapperStyle={{ width: '33%' }}
            mask={false}
            maskClosable={false}
            placement="right"
            closable={false}
            autoFocus={false}
        >
            <DrawerContent>
                <FieldHeaderWrapper>History for table</FieldHeaderWrapper>
                <TimelineTabSubheader>Search & Filter Placeholder</TimelineTabSubheader>
                <ChangeTransactionList>
                    {timelineResult.data?.getTimeline?.changeTransactions.map((changeTransaction) => (
                        <ChangeTransaction key={changeTransaction.versionStamp} changeTransaction={changeTransaction} />
                    ))}
                </ChangeTransactionList>
            </DrawerContent>
        </StyledDrawer>
    );
};

export default HistorySidebar;
