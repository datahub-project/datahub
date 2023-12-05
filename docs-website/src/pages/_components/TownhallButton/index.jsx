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

  let buttonText = '';
  let buttonLink = '';

  if (daysUntilLastThursday > 0 && daysUntilLastThursday <= 14) {
    const currentMonth = new Intl.DateTimeFormat('en-US', { month: 'long' }).format(today);
    buttonText = `Join ${currentMonth} Townhall! ✨`;
    buttonLink = 'http://rsvp.datahubproject.io';
  } else {
    buttonText = 'Watch Our Latest Townhall! 👀';
    buttonLink = 'http://rsvp.datahubproject.io';
  }

  return (
    <Link to={buttonLink} className={clsx('button button--primary button--md', styles.feature)}>
      {buttonText}
    </Link>
  );
};

export default TownhallButton;
