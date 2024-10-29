import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./champion-section.module.scss";
import { TwitterOutlined, LinkedinFilled, GithubFilled, GlobalOutlined } from "@ant-design/icons";

const ChampionSection = ({ people }) => {
  return (
    <div className={clsx("section", styles.section)}>
      {people.map((person, idx) => {
        const { name, image, position, bio, social, location } = person;
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
              <h6>{position}</h6>
            </div>
            <div className="card__body">
              <p>{bio}</p>
            </div>
            <div className="card__footer">
            {social.linkedin && (
              <a href={social.linkedin} target="_blank" rel="noopener noreferrer" className="logo">
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path d="M12 0c-6.627 0-12 5.373-12 12s5.373 12 12 12 12-5.373 12-12-5.373-12-12-12zm-2 16h-2v-6h2v6zm-1-6.891c-.607 0-1.1-.496-1.1-1.109 0-.612.492-1.109 1.1-1.109s1.1.497 1.1 1.109c0 .613-.493 1.109-1.1 1.109zm8 6.891h-1.998v-2.861c0-1.881-2.002-1.722-2.002 0v2.861h-2v-6h2v1.093c.872-1.616 4-1.736 4 1.548v3.359z"/>
                </svg>              
              </a>
              )}
              {social.github && (
                <a href={social.github} target="_blank" rel="noopener noreferrer" className= "logo">
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">
                  <rect width="100%" height="100%" fill="transparent" />
                  <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
                </svg>                
                </a>
              )}
              {social.twitter && (
                <a href={social.twitter} target="_blank" rel="noopener noreferrer" className= "logo">
                  <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">
                  <path d="M12 0c-6.627 0-12 5.373-12 12s5.373 12 12 12 12-5.373 12-12-5.373-12-12-12zm6.066 9.645c.183 4.04-2.83 8.544-8.164 8.544-1.622 0-3.131-.476-4.402-1.291 1.524.18 3.045-.244 4.252-1.189-1.256-.023-2.317-.854-2.684-1.995.451.086.895.061 1.298-.049-1.381-.278-2.335-1.522-2.304-2.853.388.215.83.344 1.301.359-1.279-.855-1.641-2.544-.889-3.835 1.416 1.738 3.533 2.881 5.92 3.001-.419-1.796.944-3.527 2.799-3.527.825 0 1.572.349 2.096.907.654-.128 1.27-.368 1.824-.697-.215.671-.67 1.233-1.263 1.589.581-.07 1.135-.224 1.649-.453-.384.578-.87 1.084-1.433 1.489z"/>
                  </svg>
                </a>
              )}
              {social.web && (
                <a href={social.web} target="_blank" rel="noopener noreferrer" className= "logo">
                  <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">
                  <path d="M12 0c-6.627 0-12 5.373-12 12s5.373 12 12 12 12-5.373 12-12-5.373-12-12-12zm10 12c0 .685-.07 1.354-.202 2h-3.853c.121-1.283.129-2.621 0-4h3.853c.132.646.202 1.315.202 2zm-.841-4h-3.5c-.383-1.96-1.052-3.751-1.948-5.278 2.435.977 4.397 2.882 5.448 5.278zm-5.554 0h-2.605v-5.658c1.215 1.46 2.117 3.41 2.605 5.658zm-4.605-5.658v5.658h-2.605c.488-2.248 1.39-4.198 2.605-5.658zm0 7.658v4h-2.93c-.146-1.421-.146-2.577 0-4h2.93zm0 6v5.658c-1.215-1.46-2.117-3.41-2.605-5.658h2.605zm2 5.658v-5.658h2.605c-.488 2.248-1.39 4.198-2.605 5.658zm0-7.658v-4h2.93c.146 1.421.146 2.577 0 4h-2.93zm-4.711-11.278c-.896 1.527-1.565 3.318-1.948 5.278h-3.5c1.051-2.396 3.013-4.301 5.448-5.278zm-6.087 7.278h3.853c-.121 1.283-.129 2.621 0 4h-3.853c-.132-.646-.202-1.315-.202-2s.07-1.354.202-2zm.639 6h3.5c.383 1.96 1.052 3.751 1.948 5.278-2.435-.977-4.397-2.882-5.448-5.278zm12.87 5.278c.896-1.527 1.565-3.318 1.948-5.278h3.5c-1.051 2.396-3.013 4.301-5.448 5.278z"/>
                  </svg>
                </a>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
};

export default ChampionSection;
