import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

@st.cache_resource
def get_database_connection():
    DATABASE_URL = os.getenv("DB_URL") 
    if not DATABASE_URL:
        st.error("Environment variable DB_URL is not set.")
        return None
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        st.error(f"Error creating database engine: {e}")
        return None


@st.cache_data
def execute_query(query):
    engine = get_database_connection()
    if engine:
        try:
            with engine.connect() as connection:
                return pd.read_sql(query, connection)
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


def fetch_session_durations():
    query = """
    WITH session_durations AS (
        SELECT 
            properties->>'$session_id' AS session_id,
            EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) AS session_duration_seconds,
            MIN(timestamp) AS min_timestamp
        FROM 
            public.posthogevents
        WHERE 
            properties->>'$session_id' IS NOT NULL
        GROUP BY 
            properties->>'$session_id'
    )
    SELECT 
        EXTRACT(YEAR FROM min_timestamp) AS year,
        EXTRACT(MONTH FROM min_timestamp) AS month,
        FLOOR(AVG(session_duration_seconds) / 60) AS average_duration_minutes,
        MOD(AVG(session_duration_seconds), 60) AS average_duration_seconds
    FROM 
        session_durations
    GROUP BY 
        year, month
    ORDER BY 
        year, month;
    """
    return execute_query(query)

def fetch_monthly_active_user():
    query = """
    WITH guest_sessions AS (
    -- Find guest user sessions with session counts greater than 5  -2
    SELECT
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month,
        COUNT(properties->>'$session_id') AS session_count,
        COALESCE(
            properties->>'userName',
            properties->>'loggedInUserName',
            properties->'user'->>'name',
            'guest_user'  -- Treat NULL users as 'guest_user'
        ) AS user_name,
        properties->>'$session_id' AS session_id  -- Track unique session
    FROM 
        public.posthogevents
    WHERE
        properties->>'$session_id' IS NOT NULL
        AND (
            COALESCE(
                properties->>'userName', 
                properties->>'loggedInUserName', 
                properties->'user'->>'name'
            ) IS NULL  -- Only for guest users
            OR properties->>'userName' NOT IN ('La Christa Eccles', 'Winston Manuel Vijay A', 'Abarna Visvanathan', 'Winston Manuel Vijay')
        )
    GROUP BY
        EXTRACT(YEAR FROM timestamp), 
        EXTRACT(MONTH FROM timestamp),
        properties->>'$session_id', 
        COALESCE(
            properties->>'userName',
            properties->>'loggedInUserName',
            properties->'user'->>'name',
            'guest_user'  -- Treat NULL users as 'guest_user'
        )
    HAVING 
        COUNT(properties->>'$session_id') > 5  -- Only include guest sessions with more than 5 occurrences -3
    ),
    active_users AS (
        -- Find active users (non-guest users)
        SELECT
            EXTRACT(YEAR FROM timestamp) AS year,
            EXTRACT(MONTH FROM timestamp) AS month,
            COUNT(DISTINCT
                COALESCE(
                    properties->>'userName', 
                    properties->>'loggedInUserName', 
                    properties->'user'->>'name',
                    'guest_user'
                )
            ) AS active_user_count
        FROM 
            public.posthogevents
        WHERE
            properties->>'$session_id' IS NOT NULL
            AND (
                COALESCE(
                    properties->>'userName', 
                    properties->>'loggedInUserName', 
                    properties->'user'->>'name'
                ) NOT IN ('La Christa Eccles', 'Winston Manuel Vijay A', 'Abarna Visvanathan', 'Winston Manuel Vijay')
                OR properties->>'userName' IS NULL
            )
        GROUP BY
            EXTRACT(YEAR FROM timestamp), 
            EXTRACT(MONTH FROM timestamp)
    )

    -- Now combine both guest_sessions and active_users
    SELECT
        gs.year,
        gs.month,
        COUNT(DISTINCT gs.session_id) AS guest_user_count,  -- Count unique guest user sessions
        au.active_user_count
    FROM
        guest_sessions gs
    JOIN
        active_users au
    ON
        gs.year = au.year
        AND gs.month = au.month
    GROUP BY
        gs.year,
        gs.month,
        au.active_user_count
    ORDER BY 
        gs.year, gs.month;
    """
    return execute_query(query)

def fetch_project_data():
    query = """
    SELECT
        TO_CHAR(p."createdAt", 'Mon YYYY') AS month_year,  
        COUNT(DISTINCT p."uid") AS new_entries,             
        COUNT(DISTINCT p2."uid") AS existing_entries,       
        COUNT(DISTINCT p."uid") + COALESCE(COUNT(DISTINCT p2."uid"), 0) AS total_entries  
    FROM 
        public."Project" p
    LEFT JOIN 
        public."Project" p2 
        ON p2."createdAt" < DATE_TRUNC('month', p."createdAt")  
    WHERE 
        p."createdAt" IS NOT NULL
        AND p."isDeleted" = FALSE  -- Exclude deleted projects
        AND (p2."isDeleted" = FALSE OR p2."isDeleted" IS NULL)  
    GROUP BY 
        TO_CHAR(p."createdAt", 'Mon YYYY')  
    ORDER BY 
        MIN(p."createdAt");
    """
    return execute_query(query)

def fetch_team_data():
    query = """
    SELECT
        TO_CHAR(p."createdAt", 'Mon YYYY') AS month_year,  
        COUNT(DISTINCT p."uid") AS new_entries,             
        COUNT(DISTINCT p2."uid") AS existing_entries,       
        COUNT(DISTINCT p."uid") + COALESCE(COUNT(DISTINCT p2."uid"), 0) AS total_entries  
    FROM 
        public."Team" p
    LEFT JOIN 
        public."Team" p2 
        ON p2."createdAt" < DATE_TRUNC('month', p."createdAt")  
    WHERE 
        p."createdAt" IS NOT NULL
    GROUP BY 
        TO_CHAR(p."createdAt", 'Mon YYYY')  
    ORDER BY 
        MIN(p."createdAt");
    """
    return execute_query(query)

def fetch_member_data():
    query = """
    SELECT
        TO_CHAR(p."createdAt", 'Mon YYYY') AS month_year,  
        COUNT(DISTINCT p."uid") AS new_entries,            
        COUNT(DISTINCT p2."uid") AS existing_entries,       
        COUNT(DISTINCT p."uid") + COALESCE(COUNT(DISTINCT p2."uid"), 0) AS total_entries  
    FROM 
        public."Member" p
    LEFT JOIN 
        public."Member" p2 
        ON p2."createdAt" < DATE_TRUNC('month', p."createdAt")  
    WHERE 
        p."createdAt" IS NOT NULL
    GROUP BY 
        TO_CHAR(p."createdAt", 'Mon YYYY') 
    ORDER BY 
        MIN(p."createdAt");
    """
    return execute_query(query)


def fetch_OH_data():
    query = """
            SELECT 
                EXTRACT(YEAR FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp) AS year,
                EXTRACT(MONTH FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp) AS month,
                CASE 
                    WHEN p."event" = 'irl-guest-list-table-office-hours-link-clicked' THEN 'IRL Page'
                    WHEN p."event" = 'member-officehours-clicked' THEN 'Member Page'
                    WHEN p."event" = 'team-officehours-clicked' THEN 'Team Page'
                    ELSE 'Unknown'
                END AS page_type,
                COUNT(*) AS interaction_count,
                EXTRACT(YEAR FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp) || '-' ||
                LPAD(EXTRACT(MONTH FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp)::text, 2, '0') AS month_year
            FROM 
                public.posthogevents p
            LEFT JOIN 
                public."Member" sm ON COALESCE(
                    (p.properties->>'userUid'),
                    (p.properties->>'loggedInUserUid'),
                    (p.properties->>'uid')
                ) = sm.uid
            LEFT JOIN 
                public."Member" tm ON COALESCE(
                    (p.properties->>'memberUid'),
                    substring(p.properties->>'$current_url' FROM '/members/([^/]+)')
                ) = tm.uid
            LEFT JOIN 
                public."Team" t ON substring(p.properties->>'$pathname' FROM '/teams/([^/]+)') = t.uid
            WHERE 
                p."event" IN (
                    'irl-guest-list-table-office-hours-link-clicked', 
                    'member-officehours-clicked',
                    'team-officehours-clicked'
                )
            GROUP BY 
                EXTRACT(YEAR FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp),
                EXTRACT(MONTH FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp),
                p."event",
                EXTRACT(YEAR FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp) || '-' ||
                LPAD(EXTRACT(MONTH FROM COALESCE(NULLIF((p.properties ->> '$sent_at')::text, ''), '1970-01-01')::timestamp)::text, 2, '0')
            ORDER BY 
                month_year;
            """
    return execute_query(query)



def fetch_event_participation_member_data():
    query = """
        SELECT 
            pe."name" AS event_name,
            pe."startDate"::date AS event_date,
            COUNT(DISTINCT CASE WHEN eg."isHost" = true THEN eg."memberUid" END) AS host_count,
            COUNT(DISTINCT CASE WHEN eg."isSpeaker" = true THEN eg."memberUid" END) AS speaker_count,
            COUNT(DISTINCT CASE WHEN eg."isHost" = false AND eg."isSpeaker" = false THEN eg."memberUid" END) AS attendee_count,
            COUNT(DISTINCT eg."memberUid") AS total_member_count
        FROM 
            public."PLEvent" pe
        LEFT JOIN 
            public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
        LEFT JOIN 
            public."Member" m ON eg."memberUid" = m."uid"
        WHERE 
            pe."startDate" IS NOT NULL
        GROUP BY 
            pe."name", pe."startDate"::date
        ORDER BY 
            pe."startDate"::date DESC;
        """
    return execute_query(query)


def fetch_event_participation_team_data():
    query = """
            SELECT 
                pe."name" AS event_name,
                pe."startDate"::date AS event_date,
                COUNT(DISTINCT CASE WHEN eg."isHost" = true THEN eg."teamUid" END) AS host_count,
                COUNT(DISTINCT CASE WHEN eg."isSpeaker" = true THEN eg."teamUid" END) AS speaker_count,
                COUNT(DISTINCT CASE WHEN eg."isHost" = false AND eg."isSpeaker" = false THEN eg."teamUid" END) AS attendee_count,
                COUNT(DISTINCT eg."teamUid") AS total_team_count  -- Total number of distinct team in each event
            FROM 
                public."PLEvent" pe
            LEFT JOIN 
                public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
            LEFT JOIN 
                public."Team" t ON eg."teamUid" = t."uid"
            WHERE 
                pe."startDate" IS NOT null
    GROUP BY 
        pe."name", pe."startDate"::date  -- Group by the date-only version of startDate
    ORDER BY 
        pe."startDate"::date DESC; 
        """
    return execute_query(query)




def main():
    st.set_page_config(page_title="nKPI Dashboard", layout="wide")

    page = st.sidebar.radio(
        "nKPI Dashboard",
        [
            "Capital",
            "Teams",
            "Brand",
            "Network Tooling",
            "Knowledge",
            "People/Talent",
            "User/Customers",
            "Programs",
            "Projects",
            "Service Providers",
            "Other Networks"
        ]
    )

    scopes = [
          
                "https://www.googleapis.com/auth/spreadsheets"
            ]
    client_email = os.getenv("GOOGLE_SHEET_CLIENT_EMAIL")
    private_key = os.getenv("GOOGLE_SHEET_PRIVATE_KEY").replace('\\n', '\n')  # Handle multiline key format
    project_id = os.getenv("GOOGLE_SHEET_PROJECT_ID")
    credentials = Credentials.from_service_account_info(
        {
            "type": "service_account",
            "project_id": project_id,
            "private_key_id": os.getenv("GOOGLE_SHEET_PRIVATE_KEY_ID"),
            "private_key": private_key,
            "client_email": client_email,
            "client_id": os.getenv("GOOGLE_SHEET_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("GOOGLE_SHEET_CLIENT_X509_CERT_URL")
        },
            scopes=scopes
    )
    client = gspread.authorize(credentials)
    sheet_url = os.getenv("GOOGLE_SHEET_SPREADSHEET_URL")
    sheet = client.open_by_url(sheet_url)

    if page == "Capital":
        try:
            worksheet = sheet.get_worksheet(1)
            ranges = ['D1:E8', 'I1:J8', 'N1:O8', 'S1:T8']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)
            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                df1.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)

                bar1 = px.bar(
                    df1,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Amount"},
                    height=500  
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside')                 

            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)
            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])
                df2.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df2.dropna(subset=["Month-Year", "Value"], inplace=True)

                bar2 = px.bar(
                    df2,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Amount"},
                    height=500  
                )
                bar2.update_traces(texttemplate='%{text}', textposition='outside')

            data_range3 = ranges[2]
            data3 = worksheet.get_values(data_range3)
            if data3 and len(data3[0]) >= 2:
                df3 = pd.DataFrame(data3[1:], columns=data3[0])
                df3.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df3.dropna(subset=["Month-Year", "Value"], inplace=True)

                bar3 = px.bar(
                    df3,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. Of Investors"},
                    height=500  
                )
                bar3.update_traces(texttemplate='%{text}', textposition='outside') 

            data_range4 = ranges[3]
            data4 = worksheet.get_values(data_range4)
            if data4 and len(data4[0]) >= 2:
                df4 = pd.DataFrame(data4[1:], columns=data4[0])
                df4.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df4.dropna(subset=["Month-Year", "Value"], inplace=True)

                bar4 = px.bar(
                    df4,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. Of Investors"},
                    height=500 
                )
                bar4.update_traces(texttemplate='%{text}', textposition='outside')  

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Capital Raised by PL Portfolio Venture Startups")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Capital Raised by All Organizations in the Network")
                st.plotly_chart(bar2)

            col3, col4 = st.columns(2)

            with col3:
                st.subheader("Angel Investors of Network Teams")
                st.plotly_chart(bar3)
            
            with col4:
                st.subheader("VC Investors of Network Teams")
                st.plotly_chart(bar4)


        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == "Teams":

        try:
            worksheet = sheet.get_worksheet(2)

            ranges = ['D1:G9', 'K1:N8']
            data_range_stage = "V3:X13" 

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)

            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                if "Month Year" in df1.columns:
                    df1.rename(columns={"Month Year": "Month-Year"}, inplace=True)
                for col in df1.columns:
                    if col != "Month-Year":
                        df1[col] = pd.to_numeric(df1[col], errors='coerce')

                df1.dropna(subset=["Month-Year"], inplace=True)

                df1["Month-Year"] = pd.to_datetime(df1["Month-Year"], errors="coerce")

                df1["Month-Year"] = df1["Month-Year"].dt.strftime('%b %Y')

                df1.sort_values(by="Month-Year", inplace=True)

                df1_long = df1.melt(
                    id_vars="Month-Year",  
                    value_vars=[col for col in df1.columns if col != "Month-Year"], 
                    var_name="Type", 
                    value_name="Value"
                )

                df1_long = df1_long[df1_long["Value"] > 0]

                bar1 = px.bar(
                    df1_long,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    color="Type", 
                    labels={"Month-Year": "Month-Year", "Value": "No. Of Teams"},
                    height=500,
                    barmode="stack" 
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside') 

                bar1.update_layout(
                    xaxis_tickformat="%b %Y", 
                    xaxis_tickangle=360,
                )

            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)

            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])

                if "Month Year" in df2.columns:
                    df2.rename(columns={"Month Year": "Month-Year"}, inplace=True)

                for col in df2.columns:
                    if col != "Month-Year":
                        df2[col] = pd.to_numeric(df2[col], errors='coerce')

                df2.dropna(subset=["Month-Year"], inplace=True)
                df2["Month-Year"] = pd.to_datetime(df2["Month-Year"], errors="coerce")        
                df2.sort_values("Month-Year", inplace=True)

                df2_long = df2.melt(
                    id_vars="Month-Year",
                    value_vars=[col for col in df2.columns if col != "Month-Year"],
                    var_name="Type",
                    value_name="Value"
                )

                df2_long = df2_long[df2_long["Value"] > 0]

                bar2 = px.bar(
                    df2_long,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    color="Type",
                    labels={"Month-Year": "Month-Year", "Value": "No. Of Teams"},
                    height=500,
                    barmode="stack"
                )
                bar2.update_traces(texttemplate='%{text}', textposition='outside')
                bar2.update_layout(
                    xaxis=dict(
                        tickformat="%b %Y",  
                        type="date",  
                        tickmode="auto",  
                    )                )
                
                bar2.update_xaxes(
                    tickformat="%b %Y", 
                    title_text="Year-Month",
                    tickmode="linear",   
                    dtick="M1",         
                )


            data = worksheet.get_values(data_range_stage)
            if data and len(data) > 1:
                df3 = pd.DataFrame(data, columns=["Stage", "Q4 2024", "Q2 2024"])

                df3["Q4 2024"] = pd.to_numeric(df3["Q4 2024"], errors='coerce')
                df3["Q2 2024"] = pd.to_numeric(df3["Q2 2024"], errors='coerce')

                df3_long = df3.melt(
                    id_vars=["Stage"], 
                    value_vars=["Q4 2024", "Q2 2024"],
                    var_name="Quarter", 
                    value_name="Count"
                )

                bar3 = px.bar(
                    df3_long,
                    x="Stage",
                    y="Count",
                    text="Count",
                    color="Quarter",  
                    labels={"Stage": "Stage", "Count": "No. Of Teams"},
                    barmode="stack",  
                    height=500
                )
                bar3.update_traces(texttemplate='%{text}', textposition='outside')  

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Shut down, Same stage, and Moved up")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Teams by Membership Tier")
                st.plotly_chart(bar2)

            col3, col4 = st.columns(2)

            with col3:
                st.subheader("Teams by Impact Tier")
                st.plotly_chart(bar3)

        except Exception as e:
            st.error(f"An error occurred: {e}")
        
    elif page == "Brand":

        try:
            worksheet = sheet.get_worksheet(3)

            ranges = ['N1:O9', 'I1:J9', 'S1:T9']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)

            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])

                if "Month Year" in df1.columns:
                    df1.rename(columns={"Month Year": "Month-Year"}, inplace=True)

                if "Data" in df1.columns:
                    df1["Data"] = df1["Data"].apply(lambda x: float(x.replace('%', '').strip()) / 100 if isinstance(x, str) else x)

                for col in df1.columns:
                    if col != "Month-Year":
                        df1[col] = pd.to_numeric(df1[col], errors='coerce')

                df1.dropna(subset=["Month-Year"], inplace=True)

                df1["Month-Year"] = pd.to_datetime(df1["Month-Year"], errors="coerce")

                df1.sort_values(by="Month-Year", inplace=True)

                df1["Month-Year"] = df1["Month-Year"].dt.strftime('%b %Y')

                df1_long = df1.melt(
                    id_vars="Month-Year",  
                    value_vars=[col for col in df1.columns if col != "Month-Year"],
                    var_name="Type", 
                    value_name="Value"
                )

                df1_long = df1_long[df1_long["Value"] > 0]

                bar1 = px.bar(
                    df1_long,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Engagement Rate"},
                    height=500,
                    barmode="stack" 
                )

                bar1.update_traces(
                    texttemplate='%{y:.2%}',  
                    textposition='outside'
                )

                bar1.update_layout(
                    xaxis_tickformat="%b %Y", 
                    xaxis_tickangle=360,  
                    yaxis_tickformat='.0%',  
                )

            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)

            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])

                if "Month Year" in df2.columns:
                    df2.rename(columns={"Month Year": "Month-Year"}, inplace=True)

                if "Count" in df2.columns:
                    df2["Count"] = df2["Count"].apply(lambda x: int(x.replace(',', '').strip()) if isinstance(x, str) else x)

                for col in df2.columns:
                    if col != "Month-Year":
                        df2[col] = pd.to_numeric(df2[col], errors='coerce')

                df2.dropna(subset=["Month-Year"], inplace=True)

                df2["Month-Year"] = pd.to_datetime(df2["Month-Year"], errors="coerce")

                df2.sort_values(by="Month-Year", inplace=True)

                df2_long = df2.melt(
                    id_vars="Month-Year",  
                    value_vars=[col for col in df2.columns if col != "Month-Year"],  
                    var_name="Type", 
                    value_name="Value"
                )

                df2_long = df2_long[df2_long["Value"] > 0]

                df2_long["Month-Year"] = df2_long["Month-Year"].dt.strftime('%b %Y')  

                bar2 = px.bar(
                    df2_long,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. Of Audience"},
                    height=500,
                    barmode="stack"  
                )

                bar2.update_traces(texttemplate='%{text}', textposition='outside')  

                bar2.update_layout(
                    xaxis_tickformat="%b %Y",  
                    xaxis_tickangle=360,
                )

            data_range3 = ranges[2]
            data3 = worksheet.get_values(data_range3)

            if data3 and len(data3[0]) >= 2:
                df3 = pd.DataFrame(data3[1:], columns=data3[0])

                if "Month Year" in df3.columns:
                    df3.rename(columns={"Month Year": "Month-Year"}, inplace=True)

                if "Data" in df3.columns:
                    df3["Data"] = pd.to_numeric(df3["Data"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df3.columns:
                    if col != "Month-Year":
                        df3[col] = pd.to_numeric(df3[col], errors='coerce')

                df3.dropna(subset=["Month-Year", "Data"], inplace=True)

                df3["Month-Year"] = pd.to_datetime(df3["Month-Year"], errors="coerce")

                df3.sort_values(by="Month-Year", inplace=True)

                df3["Month-Year"] = df3["Month-Year"].dt.strftime('%b %Y')

                df3_long = df3.melt(
                    id_vars="Month-Year",  
                    value_vars=[col for col in df3.columns if col != "Month-Year"],  
                    var_name="Type", 
                    value_name="Value"
                )

                df3_long = df3_long[df3_long["Value"] > 0]

                bar3 = px.bar(
                    df3_long,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. Of Subscribers"},
                    height=500,
                    barmode="stack" 
                )

                bar3.update_traces(texttemplate='%{text}', textposition='outside')

                bar3.update_layout(
                    xaxis_tickformat="%b %Y",  
                    xaxis_tickangle=360                )

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Engagement Rate")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Audience Growth")
                st.plotly_chart(bar2)

            col3, col4 = st.columns(2)

            with col3:
                st.subheader("Email Subscribers")
                st.plotly_chart(bar3)

            with col4:
                st.subheader("Share of Voice")
                dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/share+of+voice(nKPI).png"
                st.image(dummy_image_url,  width=900)

        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'Network Tooling':
        st.subheader("Monthly User Activity")
        df = fetch_monthly_active_user()

        df['month'] = df['month'].astype(int)  
        df['year'] = df['year'].astype(int)    

        df['Month-Year'] = df.apply(lambda row: pd.to_datetime(f"{row['month']}-01-{row['year']}", format="%m-%d-%Y").strftime("%b %Y"), axis=1)

        df_long = df.melt(id_vars=['Month-Year'], value_vars=['guest_user_count', 'active_user_count'],
                        var_name='Type', value_name='user_count')

        df_long['Type'] = df_long['Type'].map({
            'guest_user_count': 'Visitor',
            'active_user_count': 'Logged-In'        })

        fig = px.bar(
            df_long,
            x='Month-Year',
            y='user_count',
            color='Type',
            labels={'Month-Year': 'Month-Year', 'user_count': 'User Count'},
            color_discrete_map={'Visitor': 'blue', 'Logged-In': 'orange'},
            barmode='stack',
        )

        fig.update_traces(texttemplate='%{y}', textposition='outside')

        fig.update_layout(
            xaxis_tickmode='array',
            xaxis_tickvals=df['Month-Year'].unique(),
            xaxis_ticktext=df['Month-Year'].unique(),
            xaxis_tickangle=360 
        )

        st.plotly_chart(fig)
       
        st.subheader("Avg Session Duration")

        session_data = fetch_session_durations()

        if not session_data.empty:
            session_data["average_duration_combined"] = (
                session_data["average_duration_minutes"] + session_data["average_duration_seconds"] / 60
            )

            session_data["year_month"] = pd.to_datetime(session_data[["year", "month"]].assign(day=1))

            fig = px.line(
                session_data,
                x="year_month",
                y="average_duration_combined",
                labels={
                    "year_month": "Year-Month",
                    "average_duration_combined": "Avg Duration (minutes.seconds)"
                },
                markers=True
            )

            fig.update_xaxes(
                tickformat="%b %Y", 
                title_text="Year-Month",
                tickmode="linear",   
                dtick="M1",        
            )

            st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning("No data available for session durations.")

        st.subheader("Team Growth")

        df = fetch_team_data()

        df_long = df.melt(
            id_vars=["month_year"],  
            value_vars=["new_entries", "existing_entries", "total_entries"], 
            var_name="type",  
            value_name="count" 
        )

        type_mapping = {
            "new_entries": "New Entries",
            "existing_entries": "Existing Entries",
            "total_entries": "Total Entries"
        }
        df_long["type"] = df_long["type"].replace(type_mapping)

        fig = px.bar(
            df_long, 
            x="month_year", 
            y="count",  
            color="type", 
            labels={"month_year": "Month-Year", "count": "Number of Entries", "type": "Entry Type"}, 
            text_auto=True  
        )

        fig.update_layout(
            barmode="stack", 
            xaxis_title="Month-Year",
            yaxis_title="Number of Entries",
            legend_title="Entry Type",  
            showlegend=True
        )

        st.plotly_chart(fig)

        st.subheader("Member Growth") 

        df = fetch_member_data()

        df_long = df.melt(
            id_vars=["month_year"],  
            value_vars=["new_entries", "existing_entries", "total_entries"], 
            var_name="type", 
            value_name="count"  
        )

        type_mapping = {
            "new_entries": "New Entries",
            "existing_entries": "Existing Entries",
            "total_entries": "Total Entries"
        }
        df_long["type"] = df_long["type"].replace(type_mapping)

        fig = px.bar(
            df_long, 
            x="month_year",  
            y="count", 
            color="type",  
            labels={"month_year": "Month-Year", "count": "Number of Entries", "type": "Entry Type"},  
            text_auto=True  
        )

        fig.update_layout(
            barmode="stack",  
            xaxis_title="Month-Year",
            yaxis_title="Number of Entries",
            legend_title="Entry Type",  
            showlegend=True
        )

        st.plotly_chart(fig)

        st.subheader("Project Growth")

        df = fetch_project_data()

        df_long = df.melt(
            id_vars=["month_year"],  
            value_vars=["new_entries", "existing_entries", "total_entries"], 
            var_name="type", 
            value_name="count"  
        )

        type_mapping = {
            "new_entries": "New Entries",
            "existing_entries": "Existing Entries",
            "total_entries": "Total Entries"
        }
        df_long["type"] = df_long["type"].replace(type_mapping)

        fig = px.bar(
            df_long, 
            x="month_year",
            y="count", 
            color="type",  
            labels={"month_year": "Month-Year", "count": "Number of Entries", "type": "Entry Type"}, 
            text_auto=True 
        )

        fig.update_layout(
            barmode="stack",  
            xaxis_title="Month-Year",
            yaxis_title="Number of Entries",
            legend_title="Entry Type",  
            showlegend=True
        )

        st.plotly_chart(fig)

        st.subheader("NPS Feedback")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/NPS+Feedback(nKPI).png"
        st.image(dummy_image_url,  width=900)

    elif page == 'Knowledge':
        st.subheader("Office Hours Held (By Type)")

        df = fetch_OH_data()

        if not df.empty:
            df['month_year'] = pd.to_datetime(df['month_year'], format='%Y-%m')

            all_months = pd.date_range(df['month_year'].min(), df['month_year'].max(), freq='MS')

            df_full = pd.DataFrame(all_months, columns=['month_year'])
            df_full['month_year'] = pd.to_datetime(df_full['month_year'])

            df_merged = pd.merge(df_full, df, on='month_year', how='left').fillna({'interaction_count': 0})

            df_merged['month_year'] = df_merged['month_year'].dt.strftime('%b %Y')

            df_merged['month_year'] = pd.to_datetime(df_merged['month_year'], format='%b %Y')
            df_merged = df_merged.sort_values('month_year')

            df_pivot = df_merged.pivot_table(index="month_year", columns="page_type", values="interaction_count", aggfunc="sum").fillna(0)

            df_pivot.index = df_pivot.index.strftime('%b %Y')

            fig = px.bar(df_pivot,
                        x=df_pivot.index,  
                        y=df_pivot.columns,  
                        labels={"value": "Interaction Count", "month_year": "Month-Year", "page_type": "Page Type"},
                        height=400)

            fig.update_layout(
                barmode='stack',
                xaxis_tickangle=360,  
                xaxis={'tickmode': 'array', 'tickvals': df_pivot.index}
            )

            st.plotly_chart(fig)

        else:
            st.warning("No data available to display.")

        try:
            worksheet = sheet.get_worksheet(5)

            data_range_stage = "T1:V5"
            data = worksheet.get_values(data_range_stage)
            df3 = pd.DataFrame(data, columns=["Month Year", "Network Density by Member", "Network Density by Team"])

            df3["Network Density by Member"] = pd.to_numeric(df3["Network Density by Member"].replace('%', '', regex=True), errors='coerce')
            df3["Network Density by Team"] = pd.to_numeric(df3["Network Density by Team"].replace('%', '', regex=True), errors='coerce')

            df3 = df3.dropna(subset=["Network Density by Member", "Network Density by Team"])

            df3_long = df3.melt(
                id_vars=["Month Year"], 
                value_vars=["Network Density by Member", "Network Density by Team"],
                var_name="Type", 
                value_name="Count"
            )

            df3_long['Count'] = df3_long['Count'] / 100  

            bar3 = px.bar(
                df3_long,
                x="Month Year",
                y="Count",
                text="Count",
                color="Type", 
                labels={"Month Year": "Month Year", "Count": "% Network Density"},
                barmode="stack",  
                height=500
            )

            bar3.update_traces(texttemplate='%{text:.2%}', textposition='outside')

            bar3.update_layout(
                yaxis_tickformat='.0%', 
                xaxis_tickangle=360,  
            )

            st.subheader("% Network Density")
            st.plotly_chart(bar3)
        except Exception as e:
            st.error(f"An error occurred: {e}")

        st.subheader("Monthly Active Users by Contribution Type - Events")

        df = fetch_event_participation_member_data()

        if not df.empty:
            df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
            df['month_year'] = df['event_date'].dt.to_period('M').astype(str)
            
            column_mapping = {
                "host_count": "Host Count",
                "speaker_count": "Speaker Count",
                "attendee_count": "Attendee Count"
            }
            df = df.rename(columns=column_mapping)
            
            df_pivot = df.pivot_table(
                index="month_year", 
                values=["Host Count", "Speaker Count", "Attendee Count"], 
                aggfunc="sum"
            ).fillna(0)
            
            df_pivot = df_pivot.reset_index()
            
            df_pivot = df_pivot[
                (df_pivot[['Host Count', 'Speaker Count', 'Attendee Count']].sum(axis=1) > 0)
            ]
            
            if df_pivot.empty:
                st.write("No data available for the selected period.")
            else:
                df_long = df_pivot.melt(
                    id_vars="month_year",  
                    value_vars=["Host Count", "Speaker Count", "Attendee Count"], 
                    var_name="Type", 
                    value_name="Count"  
                )
                
                df_long = df_long[df_long['Count'] > 0]
                
                fig = px.bar(
                    df_long,
                    x="month_year",  
                    y="Count",  
                    color="Type",  
                    labels={"Count": "Member Count", "month_year": "Month-Year", "Type": "Participant Type"},
                    height=400
                )
                
                fig.update_layout(
                    barmode='stack',  
                    xaxis_tickangle=360,  
                    legend_title="Type"
                )

                fig.update_xaxes(
                    tickformat="%b %Y", 
                    title_text="Year-Month",
                    tickmode="linear",   
                    dtick="M1",         
                )

                
                st.plotly_chart(fig)

        st.subheader("Monthly Active Teams by Contribution Type - Events")

        df = fetch_event_participation_team_data()

        if not df.empty:
            df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
            df['month_year'] = df['event_date'].dt.to_period('M').astype(str)

            column_mapping = {
                "host_count": "Host Count",
                "speaker_count": "Speaker Count",
                "attendee_count": "Attendee Count"
            }
            df = df.rename(columns=column_mapping)

            df_pivot = df.pivot_table(
                index="month_year", 
                values=["Host Count", "Speaker Count", "Attendee Count"], 
                aggfunc="sum"
            ).fillna(0)

            df_pivot = df_pivot.reset_index()
            months_to_remove = ["2024-01", "2024-02", "2024-03", "2024-10"]
            df_pivot = df_pivot[~df_pivot['month_year'].isin(months_to_remove)]

            df_long = df_pivot.melt(
                id_vars="month_year",  
                value_vars=["Host Count", "Speaker Count", "Attendee Count"], 
                var_name="Type",
                value_name="Count"
            )

            fig = px.bar(
                df_long,
                x="month_year", 
                y="Count",  
                color="Type",  
                labels={"Count": "Team Count", "month_year": "Month-Year", "Type": "Participant Type"},
                height=400
            )

            fig.update_layout(
                barmode='stack',  
                xaxis_tickangle=360, 
                legend_title="Type"
            )

            fig.update_xaxes(
                    tickformat="%b %Y", 
                    title_text="Year-Month",
                    tickmode="linear",   
                    dtick="M1",         
                )

            st.plotly_chart(fig)
        else:
            st.warning("No data available to display.")

    elif page == 'People/Talent':
        st.subheader("# of Active Users in the Network")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/No.+Of+Avtive+Users+in+Network(nKPI).png"
        st.image(dummy_image_url,  width=900)
         
    elif page == 'User/Customers':
        st.subheader("")

    elif page == 'Programs':
        st.subheader("Number of teams participating in programs, new vs. repeat")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/No+of+team+new+and+repeat(nKPI).png"
        st.image(dummy_image_url,  width=900)
    
    elif page == 'Projects':
        st.subheader("Project Contributors by Month")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Project+Cintributors(nKPI).png"
        st.image(dummy_image_url,  width=900)

    elif page == 'Service Providers':
        st.subheader("")

    elif page == 'Other Networks':
        st.subheader("")
if __name__ == "__main__":
    main()

   