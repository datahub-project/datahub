import React, { useEffect, useState } from 'react';
import 'antd/dist/antd.css';
import { Agent } from 'https';
import { Alert } from 'antd';
import axios from 'axios';
import announceConfig from '../../conf/Announcement';

export const BannerSplash = () => {
    interface AnnouncementData {
        message: string;
        timestamp: string;
    }
    const url = announceConfig;
    const closedTime = Number(localStorage.getItem('_banner_closed_time')) || 0;
    const [data, setData] = useState<AnnouncementData>();
    const [showData, setShowData] = useState(false);
    const RetrieveData = () => {
        useEffect(() => {
            const callAPI = async () => {
                await axios
                    .get(url, {
                        httpsAgent: new Agent({
                            rejectUnauthorized: false,
                        }),
                    })
                    .then((res) => {
                        setData(res.data);
                        setShowData(closedTime < res.data.timestamp);
                        console.log(`received data from axios call is ${res.data}`);
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

    console.log(`stored timestamp is ${closedTime}`);
    console.log(`the retrieved message is ${newObj.message}`);
    console.log(`the retrieved timestamp is ${newObj.timestamp}`);
    console.log(`the timestamp is larger than localstorage time: ${closedTime < newObj.timestamp}`);
    if (showData) {
        return <Alert message="Latest Update" description={newObj.message} type="error" closable onClose={onClose} />;
    }
    return <></>;
};
