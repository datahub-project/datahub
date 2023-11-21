import React, { useState, useEffect } from 'react';
import styles from "./townhallbutton.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import clsx from "clsx";
import Link from "@docusaurus/Link";

const TownhallButton = () => {
  const [showButton, setShowButton] = useState(false);
  const [currentMonth, setCurrentMonth] = useState('');

  useEffect(() => {
    const today = new Date();
    const currentDay = today.getDate();
    const lastDayOfMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0);
    const lastThursday = lastDayOfMonth.getDate() - ((lastDayOfMonth.getDay() + 7 - 4) % 7);

    const daysUntilLastThursday = lastThursday - currentDay;

    let shouldShowButton = false;
    let monthText = '';

    if (daysUntilLastThursday > 0 && daysUntilLastThursday <= 14) {
      shouldShowButton = true;
      monthText = new Intl.DateTimeFormat('en-US', { month: 'long' }).format(today);
    }

    setShowButton(shouldShowButton);
    setCurrentMonth(monthText);
  }, []); 

  return (
      showButton && (
        <Link to="http://rsvp.datahubproject.io" className={clsx("button button--primary button--md", styles.feature)} >
            Join {currentMonth} Townhall!&nbsp;✨
        </Link>
      )
  ); 
};

export default TownhallButton;
