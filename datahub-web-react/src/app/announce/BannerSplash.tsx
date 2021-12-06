import React, { useEffect, useState } from 'react';
import 'antd/dist/antd.css';
import { Alert } from 'antd';
import axios from 'axios';

export const BannerSplash = () => {
    interface AnnouncementData {
        message: string;
        timestamp: string;
    }
    const url = 'https://xaluil.gitlab.io/announce/';
    const [data, setData] = useState<AnnouncementData>();
    const RetrieveData = () => {
        useEffect(() => {
            const callAPI = async () => {
                await axios
                    .get(url)
                    .then((res) => {
                        setData(res.data);
                    })
                    .catch((error) => {
                        console.error(error.toString());
                    }); // todo: can we have error show a default msg
            };
            callAPI();
        }, []);
    };
    const onClose = () => {
        console.log('Banner was closed.');
        const timenow = Date.now();
        localStorage.setItem('_banner_closed_time', JSON.stringify(timenow));
    };
    RetrieveData();
    const newObj = Object(data);
    const closedTime = Number(localStorage.getItem('_banner_closed_time'));
    console.log(`stored timestamp is ${closedTime}`);
    console.log(`the retrieved message is ${newObj.message}`);
    const show = closedTime < newObj.timestamp;
    if (show) {
        return <Alert message="Latest Update" description={newObj.message} type="error" closable onClose={onClose} />;
    }
    return <></>;
};
