import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./guild-section.module.scss";
import { TwitterOutlined, LinkedinFilled, GithubFilled, GlobalOutlined } from "@ant-design/icons";

const GuildSection = ({ name, badge, description, people, alttext }) => {
  return (
    <div className={clsx("section", styles.section)}>
      <div className="heading">
        <img className="badge" src={useBaseUrl(badge)} title={alttext} alt={name} style={{ transform: `rotate(${Math.floor(Math.random() * 30) - 15}deg)` }} />
        <p>{description}</p>
      </div>
      {people.map((person, idx) => {
        const { name, image, bio, social } = person;
        return (
          <div className={clsx("card", styles.card)} key={idx}>
            {image ? (
              <img className="avatar__photo" src={image} alt={name} />
            ) : (
              <img
                className="avatar__photo"
                style={{ transform: `rotate(${Math.floor(Math.random() * 30) - 15}deg)` }}
                src={useBaseUrl("/img/guild/person-placeholder.svg")}
                alt="Placeholder"
              />
            )}
            <div className="card__header">
              <h3>{name}</h3>
            </div>
            <div className="card__body">
              <p>{bio}</p>
            </div>
            <div className="card__footer">
              {social.linkedin && (
                <a href={social.linkedin} target="_blank" rel="noopener noreferrer">
                  <LinkedinFilled />
                </a>
              )}
              {social.mastodon && (
                <a href={social.mastodon} target="_blank" rel="noopener noreferrer">
                  <span role="img" aria-label="mastodon" className="anticon ">
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                      <path
                        d="M15.6013 9.59035C15.3775 10.7172 13.6513 11.956 11.6293 12.1957C10.5824 12.3156 9.55139 12.4355 8.4565 12.3875C6.65831 12.2996 5.25971 11.956 5.25971 11.956V12.4515C5.51546 14.2257 7.03393 14.3296 8.48047 14.3855C9.93501 14.4255 11.2297 14.0179 11.2297 14.0179L11.2936 15.3366C11.2936 15.3366 10.2707 15.88 8.4565 15.9839C7.4575 16.0399 6.21076 15.9599 4.76421 15.5843C1.63136 14.7452 1.08791 11.3965 1.00799 7.99196L1 5.25072C1 1.78221 3.26172 0.767228 3.26172 0.767228C4.42056 0.239759 6.38658 0 8.43252 0H8.48047C10.5264 0 12.4924 0.239759 13.6513 0.767228C13.6513 0.767228 15.913 1.78221 15.913 5.25072C15.913 5.25072 15.945 7.81613 15.6013 9.59035ZM13.2517 5.52244C13.2517 4.65931 13.0119 3.99598 12.5724 3.4765C12.1248 2.97301 11.5334 2.70927 10.7901 2.70927C9.943 2.70927 9.29565 3.03694 8.85609 3.69228L8.4565 4.39558L8.0569 3.69228C7.60935 3.03694 6.96999 2.70927 6.11485 2.70927C5.37959 2.70927 4.78819 2.97301 4.33265 3.4765C3.89309 3.99598 3.66132 4.65931 3.66132 5.52244V9.72621H5.33963V5.64232C5.33963 4.79517 5.69927 4.34762 6.42654 4.34762C7.22573 4.34762 7.62533 4.8671 7.62533 5.89007V8.11983H9.27967V5.89007C9.27967 4.8671 9.67926 4.34762 10.4865 4.34762C11.2057 4.34762 11.5654 4.79517 11.5654 5.64232V9.72621H13.2517V5.52244Z"
                        fill="currentColor"
                      />
                    </svg>
                  </span>
                </a>
              )}
              {social.twitter && (
                <a href={social.twitter} target="_blank" rel="noopener noreferrer">
                  <TwitterOutlined />
                </a>
              )}
              {social.github && (
                <a href={social.github} target="_blank" rel="noopener noreferrer">
                  <GithubFilled />
                </a>
              )}
              {social.web && (
                <a href={social.web} target="_blank" rel="noopener noreferrer">
                  <GlobalOutlined />
                </a>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
};

export default GuildSection;
