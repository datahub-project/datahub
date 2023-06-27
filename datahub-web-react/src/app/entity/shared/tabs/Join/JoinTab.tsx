import React from 'react';
import { Divider } from 'antd';
import { useGetJoinQuery } from '../../../../../graphql/join.generated';
import './JoinTab.less';
import { JoinPreview } from '../../components/styled/Join/JoinPreview';

type JoinRecord = {
    afield: string;
    bfield: string;
};

export const JoinTab = ({ properties }: { properties?: any }) => {
    const { data: entityData } = useGetJoinQuery({ variables: { urn: properties } });
    const joinView = (joinData?: any): JSX.Element => {
        let table1Name = '';
        let table2Name = '';
        let table1Urn = '';
        let table2Urn = '';
        let joinHeader = '';
        const newData = [] as JoinRecord[];
        table1Name = joinData?.properties?.datasetA?.name || '';
        table2Name = joinData?.properties?.datasetB?.name || '';
        table1Urn = joinData?.properties?.datasetA?.urn || '';
        table2Urn = joinData?.properties?.datasetB?.urn || '';
        joinData?.properties?.joinFieldMappings?.fieldMapping?.map((item) => {
            return newData.push({
                afield: item.afield,
                bfield: item.bfield,
            });
        });
        joinHeader = joinData?.editableProperties?.name || joinData?.properties?.name || '';
        return (
            <JoinPreview
                joinData={joinData}
                table1Name={table1Name}
                table2Name={table2Name}
                table1Urn={table1Urn}
                table2Urn={table2Urn}
                joinHeader={joinHeader}
                fieldMap={newData}
                joinDetails={joinData?.properties?.joinFieldMappings?.details || ''}
                prePageType="Join"
            />
        );
    };

    return (
        <>
            <div className="JoinTab">
                <div className="join-preview-div">{joinView(entityData?.join)}</div>
                <Divider className="thin-divider" />
            </div>
        </>
    );
};
