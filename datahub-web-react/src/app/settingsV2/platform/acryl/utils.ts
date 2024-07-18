export const ACRYL_PLATFORM_URN = 'urn:li:dataPlatform:acryl';

// we need our connection server urls to end with /gms, so add it for them if not already there
const formatConnectionServer = (url: string) => {
    if (url.endsWith('/gms') || url.endsWith('/gms/')) {
        return url;
    }
    if (url.endsWith('/')) {
        return `${url}gms`;
    }
    return `${url}/gms`;
};

// format the connection json object before submitting to the backend
export const getConnectionBlob = (url: string, token: string): string => {
    const formattedUrl = formatConnectionServer(url);
    const jsonObject = {
        connection: {
            server: formattedUrl,
            token,
        },
    };
    return JSON.stringify(jsonObject);
};

export const getURLFromJson = (json) => {
    const parsedJson = JSON.parse(json);
    return parsedJson.connection?.server;
};

export const getTokenFromJson = (json) => {
    const parsedJson = JSON.parse(json);
    const token = parsedJson.connection?.token;
    return token;
};

export const showToken = (token, noOfCharacters) => {
    const tokenToDisplay = `**************${token.slice(-noOfCharacters)}`;
    return tokenToDisplay;
};
