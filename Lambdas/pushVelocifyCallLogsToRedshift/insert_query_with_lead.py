insert_query = f"""
-- Step 1: Insert missing leads into the lead table
INSERT INTO public.lead (Lead_ID)
SELECT DISTINCT staging_call.Lead_ID
FROM public.staging_call AS staging_call
LEFT JOIN public.lead AS lead_table
    ON staging_call.Lead_ID = lead_table.Lead_ID
WHERE lead_table.Lead_ID IS NULL  -- Only insert if Lead_ID does not exist
  AND staging_call.Lead_ID ~ '^\d+$';  -- Ensure Lead_ID is a valid integer

-- Step 2: Insert calls into the call table
INSERT INTO public.call (
    Call_ID, Lead_ID, Broker_Name, Outcome, Call_Segment, Call_Type, Date_Time, Talk_Time, Prospect_Number, Inbound_Number
)
SELECT DISTINCT
    Call_ID,
    CASE
        WHEN Lead_ID ~ '^\d+$' THEN Lead_ID::INT  -- Valid integer Lead_ID
        ELSE NULL  -- Default invalid Lead_ID to NULL
    END AS Lead_ID,
    Broker_Name,
    Outcome,
    Call_Segment,
    Call_Type,
    CASE
        WHEN Date_Time = '' THEN NULL  -- Handle empty Date_Time
        ELSE NULLIF(Date_Time, '')::TIMESTAMP  -- Safely cast to TIMESTAMP
    END AS Date_Time,
    CASE
        WHEN Talk_Time ~ '^\d+:\d+:\d+$' THEN (
            SPLIT_PART(Talk_Time, ':', 1)::INT * 3600 +  -- Parse HH
            SPLIT_PART(Talk_Time, ':', 2)::INT * 60 +   -- Parse MM
            SPLIT_PART(Talk_Time, ':', 3)::INT         -- Parse SS
        )
        WHEN Talk_Time ~ '^\d+:\d+$' THEN (
            SPLIT_PART(Talk_Time, ':', 1)::INT * 60 +  -- Parse MM
            SPLIT_PART(Talk_Time, ':', 2)::INT        -- Parse SS
        )
        WHEN Talk_Time ~ '^\d+$' THEN Talk_Time::INT   -- Handle raw integer durations
        ELSE NULL  -- Default invalid Talk_Time to NULL
    END AS Talk_Time,
    Prospect_Number,
    Inbound_Number
FROM public.staging_call
WHERE NOT EXISTS (
    SELECT 1 FROM public.call WHERE public.call.Call_ID = public.staging_call.Call_ID
);
"""