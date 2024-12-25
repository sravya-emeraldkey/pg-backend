SELECT 
    c.Call_ID,
    c.Call_Platform,
    c.Broker_ID,
    c.Lead_ID,
    c.Call_Type,
    c.Call_Forwarded,
    c.Date_Time,
    c.Talk_Time,
    c.Outcome,
    c.Transcript,
    b.Broker_Name,
    b.phoneNumber,
    b.email,
    b.extension,
    b.state,
    b.role,
    b.Broker_Tenure
FROM 
    public.Call c
LEFT JOIN 
    public.Broker b
ON 
    c.Broker_ID = b.Broker_ID;