WITH Chat AS (
    SELECT
        NAME,
        MESSAGE_ID,
        LLM_PROMPT as Question,
        LLM_RESPONSE as Answer
    FROM {{ ref('stg_chat') }}
),
feedback AS (
    SELECT
        SCORE,
        MESSAGE_ID,
        COMMENT
    FROM {{ ref('stg_feedback') }}
),
final AS (
    SELECT
        c.Name,
        c.Question,
        c.Answer,
        f.COMMENT,
        CASE 
            WHEN f.SCORE = 0 THEN 'Negative'
            WHEN f.SCORE IS NULL THEN NULL  -- Set Feedback_Type as NULL when SCORE is NULL
            ELSE 'Positive'
        END as Feedback_Type
    FROM Chat as c
    LEFT JOIN feedback as f ON c.MESSAGE_ID = f.MESSAGE_ID
)

SELECT * FROM final
