import { useEffect } from 'react';

declare global {
    interface Window {
        zE?: (method: string, ...args: any[]) => void;
        zESettings?: {
            webWidget: {
                contactForm: {
                    title: {
                        '*': string;
                    };
                    fields?: Array<{
                        id: string | number;
                        prefill: {
                            '*': string;
                        };
                    }>;
                };
            };
        };
    }
}

export interface ZendeskConfig {
    onLoad?: () => void;
    userEmail?: string;
    userName?: string;
    customFields?: Record<string | number, string>;
}

const prefillUserData = (userEmail?: string, userName?: string) => {
    if (!window.zE) return;

    if (userEmail) {
        window.zE('webWidget', 'prefill', {
            email: { value: userEmail },
        });
    }
    if (userName) {
        window.zE('webWidget', 'prefill', {
            name: { value: userName },
        });
    }
};

const configureZendeskSettings = (customFields?: Record<string | number, string>) => {
    const customFieldsArray = customFields
        ? Object.entries(customFields).map(([id, value]) => ({
              id: Number.isNaN(Number(id)) ? id : Number(id),
              prefill: { '*': value },
          }))
        : [];

    window.zESettings = {
        webWidget: {
            contactForm: {
                title: {
                    '*': 'Contact DataHub Support',
                },
                ...(customFieldsArray.length > 0 && { fields: customFieldsArray }),
            },
        },
    };
};

const createZendeskScript = (): HTMLScriptElement => {
    const script = document.createElement('script');
    script.id = 'ze-snippet';
    script.src = 'https://static.zdassets.com/ekr/snippet.js?key=610a6804-9114-4a13-8a61-157907ed80ef';
    return script;
};

const cleanupZendesk = () => {
    const existingScript = document.getElementById('ze-snippet');
    if (existingScript) {
        existingScript.remove();
    }
    delete window.zESettings;
    delete window.zE;
};

export const useZendeskWidget = ({ onLoad, userEmail, userName, customFields }: ZendeskConfig) => {
    useEffect(() => {
        // If script already exists, just configure and open widget
        if (document.getElementById('ze-snippet')) {
            if (window.zE) {
                prefillUserData(userEmail, userName);
                window.zE('webWidget', 'open');
                onLoad?.();
            }
            return () => {};
        }

        // Configure Zendesk settings before loading script
        configureZendeskSettings(customFields);

        // Load Zendesk script
        const script = createZendeskScript();
        script.onload = () => {
            if (window.zE) {
                prefillUserData(userEmail, userName);
                window.zE('webWidget', 'open');
                onLoad?.();
            }
        };

        document.head.appendChild(script);

        return () => {
            cleanupZendesk();
        };
    }, [onLoad, userEmail, userName, customFields]);
};
