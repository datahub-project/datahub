import React from 'react';
import styles from "./townhallbutton.module.scss";
import clsx from "clsx";
import Link from "@docusaurus/Link";

const TownhallButton = () => {
  const today = new Date();
  const currentDay = today.getDate();
  const lastDayOfMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0);
  const lastThursday = lastDayOfMonth.getDate() - ((lastDayOfMonth.getDay() + 7 - 4) % 7);

  const daysUntilLastThursday = lastThursday - currentDay;

  let showButton = false;
  let currentMonth = '';

  if (daysUntilLastThursday > 0 && daysUntilLastThursday <= 14) {
    showButton = true;
    currentMonth = new Intl.DateTimeFormat('en-US', { month: 'long' }).format(today);
  }

  return (
    showButton && (
      <Link to="http://rsvp.datahubproject.io" className={clsx('button button--primary button--md', styles.feature)}>
        Join {currentMonth} Townhall!&nbsp;✨
      </Link>
    )
  );
};

export default TownhallButton;
