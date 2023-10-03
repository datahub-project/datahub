import React from 'react';
import { Divider } from 'antd';
import './JoinTab.less';
import { JoinPreview } from '../../components/styled/Join/JoinPreview';
import { useEntityData, useRefetch } from '../../EntityContext';

export const JoinTab = () => {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const joinView = (joinData?: any): JSX.Element => {
        return <JoinPreview joinData={joinData} prePageType="Join" refetch={refetch} />;
    };
    return (
        <>
            <div className="JoinTab">
                <div className="join-preview-div">{joinView(entityData)}</div>
                <Divider className="thin-divider" />
            </div>
        </>
    );
};
