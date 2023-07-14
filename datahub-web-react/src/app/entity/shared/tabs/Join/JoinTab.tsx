import React from 'react';
import { Divider } from 'antd';
import { useGetJoinQuery } from '../../../../../graphql/join.generated';
import './JoinTab.less';
import { JoinPreview } from '../../components/styled/Join/JoinPreview';

export const JoinTab = ({ properties }: { properties?: any }) => {
    const { data: entityData } = useGetJoinQuery({ variables: { urn: properties } });
    const joinView = (joinData?: any): JSX.Element => {
        return <JoinPreview joinData={joinData} prePageType="Join" />;
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
