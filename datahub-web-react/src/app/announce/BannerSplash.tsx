import React, { useEffect, useState } from 'react';
import 'antd/dist/antd.css';
import { Agent } from 'https';
import { Alert } from 'antd';
import axios from 'axios';
import ReactHtmlParser from 'react-html-parser';
import { WhereAmI } from '../home/whereAmI';

export const BannerSplash = () => {
    interface AnnouncementData {
        message: string;
        timestamp: string;
    }
    const urlBase = WhereAmI();
    const publishUrl = `${urlBase}custom/announce`;
    console.log(`the final url is ${publishUrl}`);

    const closedTime = Number(localStorage.getItem('_banner_closed_time')) || 0;
    const [data, setData] = useState<AnnouncementData>();
    const [showData, setShowData] = useState(false);
    useEffect(() => {
        const fetchData = () => {
            return axios
                .get(publishUrl, {
                    httpsAgent: new Agent({
                        rejectUnauthorized: false,
                    }),
                    headers: {
                        // 'Access-Control-Allow-Origin': 'http://127.0.0.1:3000',
                        // 'Access-Control-Allow-Methods': 'GET',
                        // 'Content-Type': 'application/json',
                        // 'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                        // crossDomain: true,
                    },
                })
                .then((res) => {
                    setData(res.data);
                    setShowData(closedTime < res.data.timestamp);
                    // console.log(`received data from axios call is ${res.data}`);
                })
                .catch((error) => {
                    console.log(error.toString());
                }); // todo: can we have error show a default msg
        };
        fetchData();
    }, [publishUrl, closedTime]);

    const onClose = () => {
        console.log('Banner was closed.');
        const timenow = Date.now();
        localStorage.setItem('_banner_closed_time', JSON.stringify(timenow));
    };

    const newObj = Object(data);

    // console.log(`stored timestamp is ${closedTime}`);
    // console.log(`the retrieved message is ${newObj.message}`);
    // console.log(`the retrieved timestamp is ${newObj.timestamp}`);
    console.log(`the timestamp is larger than localstorage time: ${closedTime < newObj.timestamp}`);
    if (showData) {
        return (
            <Alert
                message="Latest Update"
                description={ReactHtmlParser(newObj.message)}
                type="error"
                closable
                onClose={onClose}
            />
        );
    }
    return <></>;
};
