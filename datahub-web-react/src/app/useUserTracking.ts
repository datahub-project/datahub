import { useEffect, useState } from 'react';
import { useAppConfig } from './useAppConfig';

/**
 * Will turn on user tracking in our third party tool (hotjar) if the env variable is enabled
 */
export function useUserTracking() {
    const appConfig = useAppConfig();
    const [isHotjarInitialized, setIsHotjarInitialized] = useState(false);
    const { userTrackingEnabled } = appConfig.config.telemetryConfig;

    useEffect(() => {
        if (userTrackingEnabled && !isHotjarInitialized) {
            const script = document.createElement('script');
            script.innerHTML = `(function(h,o,t,j,a,r){
                h.hj=h.hj||function(){(h.hj.q=h.hj.q||[]).push(arguments)};
                h._hjSettings={hjid:3505882,hjsv:6};
                a=o.getElementsByTagName('head')[0];
                r=o.createElement('script');r.async=1;
                r.src=t+h._hjSettings.hjid+j+h._hjSettings.hjsv;
                a.appendChild(r);
            })(window,document,'https://static.hotjar.com/c/hotjar-','.js?sv=');`;
            document.head.appendChild(script);
            setIsHotjarInitialized(true);
        }
    }, [isHotjarInitialized, userTrackingEnabled]);
}
