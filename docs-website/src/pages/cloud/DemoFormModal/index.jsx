import React, { useEffect } from 'react';
import styles from "./styles.module.scss";
import DemoForm from '../DemoForm';

const DemoFormModal = ({ formId, handleCloseModal }) => {

  return (
    <div className={styles.modal}>
    <div className={styles.modalContent}>
      <button className={styles.closeButton} onClick={handleCloseModal}>✕</button>
      <DemoForm formId={formId} />
    </div>
  </div>
  );
};

export default DemoFormModal;
