import { Location } from 'history';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Prompt, useHistory } from 'react-router';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

interface Props {
    enableTabClosingHandling?: boolean;
    enableRedirectHandling?: boolean;
    confirmationModalTitle?: string;
    confirmModalContent?: React.ReactNode;
    confirmButtonText?: string;
    closeButtonText?: string;
    /**
     * By default the primary (filled) button keeps the user on the page and the secondary button
     * discards. Set this to put the destructive action on the primary button instead, rendered in
     * red, with the secondary button keeping the user on the page. Dismissing the dialog (ESC,
     * backdrop, X) always keeps the user on the page regardless of this setting.
     */
    isDiscardPrimary?: boolean;
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
    confirmModalContent,
    confirmButtonText,
    closeButtonText,
    isDiscardPrimary = false,
}: React.PropsWithChildren<Props>) {
    const { t } = useTranslation('shared.confirmation');
    const { t: tc } = useTranslation('common.actions');
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

    const stayOnPage = useCallback(() => {
        setIsConfirmationShown(false);
        setIsRedirectConfirmed(false); // restore redirect handling
    }, []);

    const dismissRedirectConfirmation = useCallback(() => setIsRedirectConfirmationShown(false), []);

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
                modalTitle={confirmationModalTitle ?? t('unsavedChanges.title')}
                modalText={confirmModalContent ?? t('unsavedChanges.text')}
                closeButtonColor="gray"
                handleConfirm={isDiscardPrimary ? () => onConfirmHandler?.() : stayOnPage}
                confirmButtonText={confirmButtonText ?? tc('continue')}
                handleClose={isDiscardPrimary ? stayOnPage : () => onConfirmHandler?.()}
                closeButtonText={closeButtonText ?? tc('exit')}
                isDeleteModal={isDiscardPrimary}
                // onCancel maps to the primary handler when set, so leave it off in discard-primary
                // mode to keep ESC/backdrop on the non-destructive path.
                closeOnPrimaryAction={!isDiscardPrimary}
            />

            {enableRedirectHandling && (
                <>
                    <Prompt when={isDirty} message={onRedirectHandler} />

                    <ConfirmationModal
                        isOpen={isRedirectConfirmationShown}
                        modalTitle={confirmationModalTitle ?? t('unsavedChanges.title')}
                        modalText={confirmModalContent ?? t('unsavedChanges.text')}
                        closeButtonColor="gray"
                        handleConfirm={isDiscardPrimary ? onRedirectConfirm : dismissRedirectConfirmation}
                        confirmButtonText={confirmButtonText ?? tc('continue')}
                        handleClose={isDiscardPrimary ? dismissRedirectConfirmation : onRedirectConfirm}
                        closeButtonText={closeButtonText ?? tc('exit')}
                        isDeleteModal={isDiscardPrimary}
                        closeOnPrimaryAction={!isDiscardPrimary}
                    />
                </>
            )}
        </DiscardUnsavedChangesConfirmationContext.Provider>
    );
}
