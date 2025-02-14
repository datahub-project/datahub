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
import quickstartContent from "./quickstartContent";
import { motion, useScroll, useTransform} from 'framer-motion';

const QuickstartContent = ({}) => {
  const scrollableElement = useRef(null)
  const { scrollYProgress } = useScroll({
    target: scrollableElement,
    offset: ["start end", "end end"]
  })
  const scaleBar = useTransform(scrollYProgress, [0, 0.2, .9, 1], [0, 0, .8, 1]);
  const opacityBar = useTransform(scrollYProgress, [0, 0.2, 0.4], [0, 0, 1]);

  return (
    <div className={clsx("quickstart", styles.quickstart)}>
      <motion.div className="quickstart__header"
        initial={{
          opacity: 0,
          scale: .8,
          y: 50,
        }}
        exit={{
          opacity: 0,
          scale: .9,
          y: -50
        }}
        whileInView={{
          opacity: 1,
          scale: 1,
          y: 0,
          transition: {
            delay: 0,
            duration: .75
          }
        }}
        viewport={{ once: true, amount: 'full' }}
      >
        <div className="quickstart__title">The only platform you need.</div>
        <div className="quickstart__subtitle">
          Unified discovery, observability, and governance for data and AI.
        </div>
      </motion.div>
      <div
        className="quickstart__container"
        id="quickstart__container"
        ref={scrollableElement}
      >
        <motion.div
          className="quickstart__bar"
          style={{  scaleY: scaleBar, opacity: opacityBar }}
        />
        {quickstartContent.map((data, idx) => (
          <motion.div key={idx} className="quickstart__content"
            initial={{
              opacity: 0,
              scale: .9,
              y: 50,
            }}
            exit={{
              opacity: 0,
              scale: .9,
              y: -50
            }}
            whileInView={{
              opacity: 1,
              scale: 1,
              y: 0,
              transition: {
                delay: 0,
                duration: .75
              }
            }}
            viewport={{ once: true, amount: .4 }}
          >
            <div className="quickstart__text">
              <div className="quickstart__text__label">{data.heading}</div>
              <div className="quickstart__text__head">{data.title}</div>
              <div className="quickstart__text__desc">
                <p dangerouslySetInnerHTML={{ __html: data.description }} />
                <a className="learn_more" href={data.link}>Learn More →</a>
              </div>
            </div>
            <div className="quickstart__img">
              <img src={useBaseUrl(data.image)} />
            </div>
          </motion.div>
        ))}
      </div>
    </div>
  );
};

export default QuickstartContent;