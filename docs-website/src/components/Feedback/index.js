import React, { useState } from "react";
import { supabase } from "./supabase";
import styles from "./styles.module.scss";
import { LikeOutlined, DislikeOutlined, CheckCircleOutlined } from "@ant-design/icons";

const Feedback = ({ page }) => {
  const [reaction, setReaction] = useState(null);

  const handleReaction = async (selectedReaction) => {
    console.log("Button clicked:", selectedReaction);
    try {
      const { data, error } = await supabase.from("feedback").insert([
        {
          page: window.location.href,
          reaction: selectedReaction,
        },
      ]);

      if (error) {
        console.error("Error submitting feedback:", error);
        return;
      }

      setReaction(selectedReaction);
    } catch (error) {
      console.error("Error submitting feedback:", error);
    }
  };

  return (
    <div className={styles.feedbackWrapper}>
      <div className={styles.feedbackWidget}>
        {reaction === null ? (
          <div className={styles.feedbackButtons}>
            <strong>Is this page helpful?</strong>
            <div>
              <button onClick={() => handleReaction("thumbs_up")}>
                <LikeOutlined />
              </button>
              <button onClick={() => handleReaction("thumbs_down")}>
                <DislikeOutlined />
              </button>
            </div>
          </div>
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
