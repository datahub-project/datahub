import { Drawer } from 'antd';
import React from 'react';
import styled from 'styled-components';
import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { useEntityData } from '../../../../EntityContext';
import { useGetTimelineQuery } from '../../../../../../../graphql/timeline.generated';
import { ChangeCategoryType } from '../../../../../../../types.generated';
import ChangeTransaction from './ChangeTransaction';
import { REDESIGN_COLORS } from '../../../../constants';

const StyledDrawer = styled(Drawer)`
    &&& .ant-drawer-body {
        padding: 0;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        height: 100%;
        overflow-x: hidden;
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
    align-items: center;
    background: ${REDESIGN_COLORS.BACKGROUND_PURPLE};
    color: #fff;
    font-size: 14px;
    font-weight: 700;
`;

const ChangeTransactionList = styled.div`
    display: flex;
    flex-direction: column;
    padding: 26px;
`;

const CloseIcon = styled.div`
    display: flex;
    &&:hover {
        cursor: pointer;
        stroke: ${REDESIGN_COLORS.WHITE};
    }
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
                <FieldHeaderWrapper>
                    Change History
                    <CloseIcon onClick={() => onClose()}>
                        <CloseOutlinedIcon />
                    </CloseIcon>
                </FieldHeaderWrapper>

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
