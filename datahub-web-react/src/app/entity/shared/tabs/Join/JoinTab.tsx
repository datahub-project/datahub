import React from 'react';
import { Divider } from 'antd';
import './JoinTab.less';
import { JoinPreview } from '../../components/styled/Join/JoinPreview';
import { useEntityData } from '../../EntityContext';

export const JoinTab = () => {
    const { entityData } = useEntityData();
    const joinView = (joinData?: any): JSX.Element => {
        return <JoinPreview joinData={joinData} prePageType="Join" />;
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
