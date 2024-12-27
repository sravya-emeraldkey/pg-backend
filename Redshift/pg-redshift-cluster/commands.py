CREATE TABLE public.Stage (
    Stage_ID INT PRIMARY KEY,
    Stage_Name VARCHAR(255),
    Description TEXT
);

CREATE TABLE public.Lead (
    Lead_ID INT PRIMARY KEY,
    Source VARCHAR(255),
    Lead_Status VARCHAR(255),
    Lead_Score VARCHAR(255),
    Stage_ID INT REFERENCES public.Stage(Stage_ID),
    Creation_Date TIMESTAMP,
    Notes TEXT
);


CREATE TABLE public.Customer (
    Customer_ID INT PRIMARY KEY,
    Lead_ID INT REFERENCES public.Lead(Lead_ID),
    Name VARCHAR(255),
    Contact_Info VARCHAR(255),
    Region VARCHAR(255),
    Last_Purchase TIMESTAMP,
    Revenue INT,
    Investment_Type VARCHAR(255)
);

CREATE TABLE public.Call (
    Call_ID VARCHAR PRIMARY KEY,
    Call_Platform VARCHAR(255),
    Broker_ID VARCHAR REFERENCES public.Broker(Broker_ID) NULL,
    Lead_ID INT REFERENCES public.Lead(Lead_ID) NULL,
    Call_Type VARCHAR(255),
    Call_Forwarded BOOLEAN,
    Date_Time TIMESTAMP,
    Talk_Time INT,
    Outcome VARCHAR(255)
);

CREATE TABLE public.Broker (
    Broker_ID VARCHAR PRIMARY KEY,
    Broker_Name VARCHAR(255),
    phoneNumber VARCHAR(255),
    email VARCHAR(255),
    extension VARCHAR(255),
    state VARCHAR(255),
    role VARCHAR(255)
);

CREATE TABLE public.Transcript (
    Transcript_ID VARCHAR PRIMARY KEY,
    Call_ID VARCHAR REFERENCES public.Call(Call_ID) NULL,
    Transcript TEXT
);