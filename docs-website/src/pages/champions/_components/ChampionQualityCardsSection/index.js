import React from "react";
import styles from "./championqualitycardssection.module.scss";
import clsx from "clsx";
import { CodeTwoTone, HeartTwoTone, SoundTwoTone } from "@ant-design/icons";

const ChampionQualityCardsSection = () => {
  return (
    <div>
    <div class={clsx(styles.subtitle)}>Our Champions...</div>
    <div class={clsx("row section", styles.section)}>
      <div class={clsx("card col col-4", styles.card)}>
        <div class="card-body">
          <h3>
          <CodeTwoTone />  
          </h3>
          <h4 class="card-title">Contribute to our code</h4>
          <p class="card-text">Enhance our projects by contributing to our GitHub repositories.</p>
        </div>
      </div>
      <div class={clsx("card col col-4", styles.card)}>
        <div class="card-body">
          <h3>
          <HeartTwoTone />
          </h3>
          <h4 class="card-title"> Help out the community</h4>
          <p class="card-text">Support our community by actively participating in our Slack channels</p>
        </div>
      </div>
      <div class={clsx("card col col-4", styles.card)}>
      <div class="card-body">
        <h3>
          <SoundTwoTone />
          </h3>
        <h4 class="card-title"> Share the exprience</h4>
        <p class="card-text">Inspire others by sharing your adoption story through blogs or town hall sessions.</p>
      </div>
    </div>
    </div>
    </div>
  );

};

export default ChampionQualityCardsSection;