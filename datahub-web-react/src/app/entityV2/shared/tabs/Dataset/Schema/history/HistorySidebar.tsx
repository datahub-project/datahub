/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Drawer } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useGetSiblingPlatforms } from '@app/entity/shared/siblingUtils';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import ChangeTransactionView, {
    ChangeTransactionEntry,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeTransactionView';

import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { ChangeCategoryType, ChangeTransaction, DataPlatform, SemanticVersionStruct } from '@types';

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
    background: ${(p) => p.theme.styles['primary-color']};
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

interface Props {
    open: boolean;
    onClose: () => void;
    urn: string;
    siblingUrn?: string;
    versionList: SemanticVersionStruct[];
    hideSemanticVersions: boolean;
}

const HistorySidebar = ({ open, onClose, urn, siblingUrn, versionList, hideSemanticVersions }: Props) => {
    const { data: entityTimelineData } = useGetTimelineQuery({
        variables: {
            input: {
                urn,
                changeCategories: [ChangeCategoryType.TechnicalSchema, ChangeCategoryType.Documentation],
            },
        },
    });
    const { data: siblingTimelineData } = useGetTimelineQuery({
        skip: !siblingUrn,
        variables: {
            input: {
                urn: siblingUrn || '',
                changeCategories: [ChangeCategoryType.TechnicalSchema, ChangeCategoryType.Documentation],
            },
        },
    });

    const { entityPlatform, siblingPlatform } = useGetSiblingPlatforms();
    const transactionEntries: ChangeTransactionEntry[] = [
        ...(entityTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
            makeTransactionEntry(transaction, hideSemanticVersions ? [] : versionList, entityPlatform ?? undefined),
        ) || []),
        ...(siblingTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
            makeTransactionEntry(transaction, [], siblingPlatform ?? undefined),
        ) || []),
    ].sort((a, b) => a.transaction.timestampMillis - b.transaction.timestampMillis);

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
                    {transactionEntries
                        .map((entry) => <ChangeTransactionView key={entry.transaction.versionStamp} {...entry} />)
                        .reverse()}
                </ChangeTransactionList>
            </DrawerContent>
        </StyledDrawer>
    );
};

function makeTransactionEntry(
    transaction: ChangeTransaction,
    versionList: SemanticVersionStruct[],
    platform?: DataPlatform,
): ChangeTransactionEntry {
    return {
        transaction,
        platform,
        semanticVersion:
            versionList.find((v) => v.semanticVersionTimestamp === transaction.timestampMillis)?.semanticVersion ??
            undefined,
    };
}

export default HistorySidebar;
