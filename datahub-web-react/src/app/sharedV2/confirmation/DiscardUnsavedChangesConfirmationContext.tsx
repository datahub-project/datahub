import { Location } from 'history';
import React, { useCallback, useEffect, useState } from 'react';
import { Prompt, useHistory } from 'react-router';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

interface Props {
    enableTabClosingHandling?: boolean;
    enableRedirectHandling?: boolean;
    confirmationModalTitle?: string;
    confirmationModalContent?: React.ReactNode;
    confirmButtonText?: string;
    closeButtonText?: string;
}

interface ConfirmationArgs {
    onConfirm: (() => void) | undefined;
}

interface DiscardUnsavedChangesConfirmationContextType {
    setIsDirty: (isDirty: boolean) => void;
    showConfirmation: (args: ConfirmationArgs) => void;
}

const DiscardUnsavedChangesConfirmationContext = React.createContext<DiscardUnsavedChangesConfirmationContextType>({
    setIsDirty: () => {},
    showConfirmation: () => {},
});

export function useDiscardUnsavedChangesConfirmationContext() {
    return React.useContext<DiscardUnsavedChangesConfirmationContextType>(DiscardUnsavedChangesConfirmationContext);
}

export function DiscardUnsavedChangesConfirmationProvider({
    children,
    enableTabClosingHandling = true,
    enableRedirectHandling = true,
    confirmationModalTitle,
    confirmationModalContent,
    confirmButtonText,
    closeButtonText,
}: React.PropsWithChildren<Props>) {
    const [isDirty, setIsDirty] = useState<boolean>(false);
    const [isConfirmationShown, setIsConfirmationShown] = useState<boolean>(false);
    const [onConfirmHandler, setOnConfirmHandler] = useState<(() => void) | undefined>(undefined);

    const [lastRedirectLocation, setLastRedirectLocation] = useState<string | undefined>();
    const [isRedirectConfirmed, setIsRedirectConfirmed] = useState<boolean>(false);
    const [isRedirectConfirmationShown, setIsRedirectConfirmationShown] = useState<boolean>(false);

    const history = useHistory();

    // Show the browser's default confirmation on tab closing
    useEffect(() => {
        const handleBeforeUnload = (e: BeforeUnloadEvent) => {
            if (isDirty && enableTabClosingHandling) {
                e.preventDefault();
                e.returnValue = '';
            }
        };

        window.addEventListener('beforeunload', handleBeforeUnload);
        return () => window.removeEventListener('beforeunload', handleBeforeUnload);
    }, [isDirty, enableTabClosingHandling]);

    const showConfirmation = useCallback((args: ConfirmationArgs) => {
        setIsConfirmationShown(true);
        setOnConfirmHandler(() => args.onConfirm);
        setIsRedirectConfirmed(true); // prevent showing confirmation on redirect
    }, []);

    const onRedirectHandler = useCallback(
        (location: Location) => {
            if (isDirty && !isRedirectConfirmed && enableRedirectHandling) {
                setIsRedirectConfirmationShown(true);
                setLastRedirectLocation(location.pathname + location.search);
                return false; // Block redirect
            }
            return true; // Allow redirect
        },
        [isDirty, isRedirectConfirmed, enableRedirectHandling],
    );

    const onRedirectConfirm = useCallback(() => {
        setIsRedirectConfirmationShown(false);
        setIsRedirectConfirmed(true);
        // Defer redirect to the next tick
        setTimeout(() => {
            if (lastRedirectLocation) {
                history.push(lastRedirectLocation);
            }
        }, 0);
    }, [history, lastRedirectLocation]);

    return (
        <DiscardUnsavedChangesConfirmationContext.Provider value={{ setIsDirty, showConfirmation }}>
            {children}

            <ConfirmationModal
                isOpen={isConfirmationShown}
                modalTitle={confirmationModalTitle ?? 'You have unsaved changes'}
                modalText={
                    confirmationModalContent ??
                    'Exiting now will discard your changes. You can continue or exit and start over later'
                }
                closeButtonColor="gray"
                handleConfirm={() => {
                    setIsConfirmationShown(false);
                    setIsRedirectConfirmed(false); // restore redirect handling
                }}
                confirmButtonText={confirmButtonText ?? 'Continue'}
                handleClose={() => onConfirmHandler?.()}
                closeButtonText={closeButtonText ?? 'Exit'}
                closeOnPrimaryAction
            />

            {enableRedirectHandling && (
                <>
                    <Prompt when={isDirty} message={onRedirectHandler} />

                    <ConfirmationModal
                        isOpen={isRedirectConfirmationShown}
                        modalTitle={confirmationModalTitle ?? 'You have unsaved changes'}
                        modalText={
                            confirmationModalContent ??
                            'Exiting now will discard your changes. You can continue or exit and start over later'
                        }
                        closeButtonColor="gray"
                        handleConfirm={() => setIsRedirectConfirmationShown(false)}
                        confirmButtonText={confirmButtonText ?? 'Continue'}
                        handleClose={onRedirectConfirm}
                        closeButtonText={closeButtonText ?? 'Exit'}
                        closeOnPrimaryAction
                    />
                </>
            )}
        </DiscardUnsavedChangesConfirmationContext.Provider>
    );
}
