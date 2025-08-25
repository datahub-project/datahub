import { createContext, useContext } from 'react';

export type ModalContextType = {
    isInsideModal: boolean;
};

export const ModalContext = createContext<ModalContextType>({ isInsideModal: false });

export const useModalContext = () => useContext(ModalContext);
