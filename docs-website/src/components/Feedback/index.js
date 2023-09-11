import React, { useState, useMemo } from "react";
import clsx from "clsx";
import { supabase } from "./supabase";
import styles from "./styles.module.scss";
import { LikeOutlined, DislikeOutlined, CheckCircleOutlined } from "@ant-design/icons";
import { v4 as uuidv4 } from "uuid";

const Feedback = () => {
  const [reaction, setReaction] = useState(null);
  const [feedback, setFeedback] = useState("");
  const [submitted, setSubmitted] = useState(false);
  const [reactionId, setReactionId] = useState(null);

  const handleReaction = async (selectedReaction) => {
    if (reaction !== selectedReaction) {
      const uuid = uuidv4();
      try {
        const { error } = await supabase.from("reaction_feedback").insert([
          {
            id: uuid,
            page: window.location.href,
            reaction: selectedReaction,
          },
        ]);

        if (error) {
          console.error("Error submitting feedback:", error);
          return;
        }
        setReactionId(uuid);
        setReaction(selectedReaction);
      } catch (error) {
        console.error("Error submitting feedback:", error);
      }
    } else {
      setReaction(null);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if (feedback !== "" && reactionId !== null) {
        const { error } = await supabase.from("written_feedback").insert([
          {
            feedback: feedback,
            reaction_feedback_id: reactionId,
          },
        ]);

        if (error) {
          console.error("Error submitting feedback:", error);
          return;
        }
        setSubmitted(true);
      }
    } catch (error) {
      console.error("Error submitting feedback:", error);
    }
  };

  return (
    <div className={styles.feedbackWrapper}>
      <div className={styles.feedbackWidget}>
        {!submitted ? (
          <>
            <div className={styles.feedbackButtons}>
              <strong>Is this page helpful?</strong>
              <div>
                <button className={clsx(styles.feedbackButton, reaction === "positive" && styles.active)} onClick={() => handleReaction("positive")}>
                  <LikeOutlined />
                </button>
                <button className={clsx(styles.feedbackButton, reaction === "negative" && styles.active)} onClick={() => handleReaction("negative")}>
                  <DislikeOutlined />
                </button>
              </div>
            </div>
            {reaction !== null && (
              <form className={styles.feedbackForm} onSubmit={handleSubmit}>
                <textarea
                  className={styles.feedbackText}
                  value={feedback}
                  rows="4"
                  onChange={(e) => setFeedback(e.target.value)}
                  placeholder="Your feedback (optional)"
                ></textarea>
                <button className="button button--secondary">Send</button>
              </form>
            )}
          </>
        ) : (
          <div className={styles.feedbackMessage}>
            <CheckCircleOutlined />
            <strong>Thanks for your feedback!</strong>
          </div>
        )}
      </div>
    </div>
  );
};

export default Feedback;
