import React, { useEffect, useRef, useState } from "react";
import clsx from "clsx";
// import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
// import Image from "@theme/IdealImage";
// import { useColorMode } from "@docusaurus/theme-common";
// import { QuestionCircleOutlined } from "@ant-design/icons";
import styles from "./quickstartcontent.module.scss";
// import CodeBlock from "@theme/CodeBlock";
// import TownhallButton from "../TownhallButton";
// import { Section } from "../Section";
import quickstart from "./quickstartContent";

const QuickstartContent = ({}) => {
  // const { colorMode } = useColorMode();
  const [barHeight, setBarHeight] = useState(0);
  const scrollableElement = useRef();

  useEffect(() => {
    const handleScroll = () => {
      const scrollPosition = scrollableElement.current.scrollTop;
      const totalHeight =
        scrollableElement.current.scrollHeight -
        scrollableElement.current.clientHeight;
      const progressPercentage = (scrollPosition / totalHeight) * 100;
      const scaledProgressPercentage = progressPercentage * 0.5;
      setBarHeight(scaledProgressPercentage);
    };
    scrollableElement.current?.addEventListener("scroll", handleScroll);
    return () => {
      scrollableElement.current?.removeEventListener("scroll", handleScroll);
    };
  }, [scrollableElement]);

  return (
    <div className={clsx("quickstart", styles.quickstart)}>
      <div className="quickstart__header">
        <div className="quickstart__title">The only platform you need</div>
        <div className="quickstart__subtitle">
          Unified Discovery, Observability, and Governance for Data and AI.
        </div>
      </div>
      <div
        className="quickstart__container"
        id="quickstart__container"
        ref={scrollableElement}
      >
        <div
          className="quickstart__bar"
          style={{ height: `${barHeight}%`, maxHeight: "50vh" }}
        />
        {quickstart.map((data, idx) => (
          <div className="quickstart__content" key={idx}>
            <div className="quickstart__text">
              <div className="quickstart__text__label">{data.heading}</div>
              <div className="quickstart__text__head">{data.title}</div>
              <div className="quickstart__text__desc">
                <p dangerouslySetInnerHTML={{ __html: data.description }} />
                <span className="learn_more">Learn More →</span>
              </div>
            </div>
            <div className="quickstart__img">
              <img src={useBaseUrl(data.image)} />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default QuickstartContent;