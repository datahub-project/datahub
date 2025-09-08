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
    trigger?: number; // Add a trigger to force re-execution
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

const updateZendeskSettings = (customFields?: Record<string | number, string>) => {
    if (!window.zE) return;

    const customFieldsArray = customFields
        ? Object.entries(customFields).map(([id, value]) => ({
              id: Number.isNaN(Number(id)) ? id : Number(id),
              prefill: { '*': value },
          }))
        : [];

    const settingsData = {
        webWidget: {
            contactForm: {
                title: {
                    '*': 'Contact DataHub Support',
                },
                ...(customFieldsArray.length > 0 && { fields: customFieldsArray }),
            },
        },
    };
    window.zE('webWidget', 'updateSettings', settingsData);
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

export const useZendeskWidget = ({ onLoad, userEmail, userName, customFields, trigger }: ZendeskConfig) => {
    useEffect(() => {
        const existingScript = document.getElementById('ze-snippet');

        // If script already exists, update settings dynamically and open widget
        if (existingScript) {
            if (window.zE) {
                updateZendeskSettings(customFields);
                prefillUserData(userEmail, userName);
                onLoad?.();
            } else {
                console.log('window.zE is not available');
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

        // Don't cleanup on every trigger change, only on unmount
        return () => {};
    }, [onLoad, userEmail, userName, customFields, trigger]);

    // Cleanup only when component unmounts
    useEffect(() => {
        return () => {
            cleanupZendesk();
        };
    }, []);
};
