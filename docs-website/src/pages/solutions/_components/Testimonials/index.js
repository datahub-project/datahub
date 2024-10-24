import React, { useEffect, useRef, useState } from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./styles.module.scss";

const Testimonials = ({ testimonialsData }) => {
    const { title, feature1, feature2, feature1Link, feature2Link, imgSrc } = testimonialsData;
  return (
    <div className={clsx("testimonials", styles.testimonials)}>
        <div className="testimonials__content">
        <div className="testimonials__card">
            <div className="testimonials__text">
            <div className="testimonials__quote_title">
                {title}
            </div>
            <span className="testimonials__quote_description">
                Seamlessly integrated with DataHub Cloud's<br/><a href={useBaseUrl(feature1Link)} className="testimonials__quote_black">{feature1}</a> and <a href={useBaseUrl(feature2Link)} className="testimonials__quote_black">{feature2}</a> solutions.
            </span>
            </div>
            <div className="testimonials__logo">
              <img src={useBaseUrl(imgSrc)} />
            </div>
        </div>
        </div>
    </div>
  );
};

export default Testimonials;
