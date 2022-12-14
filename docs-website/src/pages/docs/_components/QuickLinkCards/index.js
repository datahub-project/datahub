import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcards.module.scss";
import QuickLinkCard from "../QuickLinkCard";


const QuickLinkCards = ({quickLinkContent}) =>
  quickLinkContent?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row row--no-gutters">
          {quickLinkContent.map((props, idx) => (
            <QuickLinkCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default QuickLinkCards;
