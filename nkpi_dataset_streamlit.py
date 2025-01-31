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
    TO_CHAR(pe."startDate", 'FMMon YYYY') AS month_year,  
    pe."uid" AS event_uid,  -- Include the event UID in the result
    'Host Count' AS Type,
    COUNT(DISTINCT CASE WHEN eg."isHost" = true THEN eg."memberUid" END) AS Count
FROM 
    public."PLEvent" pe
LEFT JOIN 
    public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
WHERE 
    pe."startDate" IS NOT NULL
GROUP BY 
    TO_CHAR(pe."startDate", 'FMMon YYYY'),
    pe."uid"

UNION ALL

SELECT 
    TO_CHAR(pe."startDate", 'FMMon YYYY') AS month_year,  
    pe."uid" AS event_uid,  -- Include the event UID in the result
    'Speaker Count' AS Type,
    COUNT(DISTINCT CASE WHEN eg."isSpeaker" = true THEN eg."memberUid" END) AS Count
FROM 
    public."PLEvent" pe
LEFT JOIN 
    public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
WHERE 
    pe."startDate" IS NOT NULL
GROUP BY 
    TO_CHAR(pe."startDate", 'FMMon YYYY'),
    pe."uid"

UNION ALL

SELECT 
    TO_CHAR(pe."startDate", 'FMMon YYYY') AS month_year,  
    pe."uid" AS event_uid,  -- Include the event UID in the result
    'Attendee Count' AS Type,
    COUNT(DISTINCT CASE WHEN eg."isHost" = false AND eg."isSpeaker" = false THEN eg."memberUid" END) AS Count
FROM 
    public."PLEvent" pe
LEFT JOIN 
    public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
WHERE 
    pe."startDate" IS NOT NULL
GROUP BY 
    TO_CHAR(pe."startDate", 'FMMon YYYY'),
    pe."uid"

ORDER BY 
    month_year ASC, 
    Type, 
    event_uid;  -- Order by event_uid as well

    """
    return execute_query(query)

def fetch_event_participation_team_data():
    query = """
            SELECT 
    TO_CHAR(pe."startDate", 'FMMon YYYY') AS month_year,  
    'Host Count' AS Type,
    COUNT(DISTINCT CASE WHEN eg."isHost" = true THEN eg."teamUid" END) AS Count
FROM 
    public."PLEvent" pe
LEFT JOIN 
    public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
WHERE 
    pe."startDate" IS NOT NULL
GROUP BY 
    TO_CHAR(pe."startDate", 'FMMon YYYY'), pe."uid"

UNION ALL

SELECT 
    TO_CHAR(pe."startDate", 'FMMon YYYY') AS month_year,  
    'Speaker Count' AS Type,
    COUNT(DISTINCT CASE WHEN eg."isSpeaker" = true THEN eg."teamUid" END) AS Count
FROM 
    public."PLEvent" pe
LEFT JOIN 
    public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
WHERE 
    pe."startDate" IS NOT NULL
GROUP BY 
    TO_CHAR(pe."startDate", 'FMMon YYYY'), pe."uid"

UNION ALL

SELECT 
    TO_CHAR(pe."startDate", 'FMMon YYYY') AS month_year,  
    'Attendee Count' AS Type,
    COUNT(DISTINCT CASE WHEN eg."isHost" = false AND eg."isSpeaker" = false THEN eg."teamUid" END) AS Count
FROM 
    public."PLEvent" pe
LEFT JOIN 
    public."PLEventGuest" eg ON pe."uid" = eg."eventUid"
WHERE 
    pe."startDate" IS NOT NULL
GROUP BY 
    TO_CHAR(pe."startDate", 'FMMon YYYY'), pe."uid"

ORDER BY 
    month_year ASC, 
    Type;

        """
    return execute_query(query)


def process_and_plot(data_range, worksheet, x_col, y_col, y_label):
    """
    Processes data from a given range, creates a DataFrame, and plots a bar chart.

    Args:
        data_range (str): The range of cells to extract data from.
        worksheet: The worksheet object.
        x_col (str): The column to use for the X-axis.
        y_col (str): The column to use for the Y-axis.
        y_label (str): Label for the Y-axis.
    Returns:
        Plotly Figure: The generated bar chart.
    """
    data = worksheet.get_values(data_range)
    if data and len(data[0]) >= 2:
        df = pd.DataFrame(data[1:], columns=data[0])
        df.rename(columns={"Month Year": "Month-Year", "Data": y_col}, inplace=True)
        df.dropna(subset=["Month-Year", y_col], inplace=True)

        if "Value" in df.columns:
            df["Value"] = df["Value"].replace({'\$': '', ',': '', '': None}, regex=True)
            df["Value"] = pd.to_numeric(df["Value"], errors='coerce')

        for col in df.columns:
            if col != "Month-Year":
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df[df["Value"] > 0]

        bar = px.bar(
            df,
            x=x_col,
            y=y_col,
            text=y_col,
            labels={x_col: "Month-Year", y_col: y_label},
            height=500
        )
        bar.update_traces(texttemplate='%{text}', textposition='outside')
        return bar
    return None


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
            "Projects",
            "Programs",
            "Service Providers",
            "Other Networks",
            "User/Customers"
        ]
    )

    scopes = [
          
                "https://www.googleapis.com/auth/spreadsheets"
            ]
    client_email = os.getenv("GOOGLE_SHEET_CLIENT_EMAIL")
    private_key = os.getenv("GOOGLE_SHEET_PRIVATE_KEY").replace('\\n', '\n')
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
            ranges = ['D1:E20', 'I1:J20', 'N1:O8', 'S1:T20']

            bar1 = process_and_plot(ranges[0], worksheet, "Month-Year", "Value", "Amount")
            bar2 = process_and_plot(ranges[1], worksheet, "Month-Year", "Value", "Amount")
            bar3 = process_and_plot(ranges[2], worksheet, "Month-Year", "Value", "No. Of Investors")
            bar4 = process_and_plot(ranges[3], worksheet, "Month-Year", "Value", "No. Of Investors")

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Capital Raised by PL Portfolio Venture Startups")
                if bar1: st.plotly_chart(bar1)

            with col2:
                st.subheader("Capital Raised by All Organizations in the Network")
                if bar2: st.plotly_chart(bar2)

            col3, col4 = st.columns(2)
            with col3:
                st.subheader("Angel Investors of Network Teams")
                if bar3: st.plotly_chart(bar3)

            with col4:
                st.subheader("VC Investors of Network Teams")
                if bar4: st.plotly_chart(bar4)

        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == "Teams":

        try:
            worksheet = sheet.get_worksheet(2)

            ranges = ['D1:G20', 'K1:N20', 'V3:X13']

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


            data = worksheet.get_values(ranges[2])
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

                df3_long = df3_long[df3_long["Count"] > 0]

                bar3 = px.bar(
                    df3_long,
                    x="Stage",
                    y="Count",
                    text="Count",
                    color="Quarter",  
                    labels={"Stage": "Stage", "Count": "No. Of Teams"},
                    barmode="group",  
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

            ranges = ['N1:O20', 'I1:J20', 'S1:T20']
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

                df1 = df1[df1["Data"] > 0]

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
                st.subheader("Share of Voice")
                dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/share+of+voice(nKPI).png"
                st.image(dummy_image_url,  width=900)

            with col2:
                st.subheader("Audience Growth")
                st.plotly_chart(bar2)

            col3, col4 = st.columns(2)

            with col3:
                st.subheader("Engagement Rate")
                st.plotly_chart(bar1)

            with col4:
                st.subheader("Email Subscribers")
                st.plotly_chart(bar3)
                
        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'Network Tooling':
        worksheet = sheet.get_worksheet(5)
        data_range2 = 'D1:F20'
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

            df2.sort_values(by="Month-Year", inplace=True)
            df2_long = df2.melt(
                id_vars="Month-Year",  
                value_vars=[col for col in df2.columns if col != "Month-Year"],  
                var_name="Type", 
                value_name="Value"
            )

            df2_long = df2_long[df2_long["Value"] > 0]
            df2_long["Month-Year"] = df2_long["Month-Year"].dt.strftime('%b %Y')  
            fig = px.bar(
                df2_long,
                x="Month-Year",
                y="Value",
                color="Type", 
                text="Value", 
                labels={"Month-Year": "Month-Year", "Value": "Count"},
                height=500,
                barmode="stack"  
            )

            fig.update_traces(texttemplate='%{text}', textposition='outside')

            fig.update_layout(
                xaxis=dict(
                    type="category",  
                    tickmode="array",  
                    tickvals=df2_long["Month-Year"]                ),
                xaxis_title="Month-Year",
                xaxis_tickangle=45,  
                yaxis_title="Count",
                showlegend=True
            )

        worksheet = sheet.get_worksheet(5)
        data_range2 = 'J1:K13'
        df = worksheet.get_values(data_range2)

        df = pd.DataFrame(df[1:], columns=df[0])
        df['Minutes'] = df['Time (Min.Sec)'].astype(str).str.split('.').apply(
            lambda x: int(x[0]) + (int(x[1]) / 60 if len(x) > 1 and x[1].isdigit() else 0) if x[0].isdigit() else None
        )
        fig_1 = px.line(df, x='Month Year', y='Minutes', markers=True,
                    labels={'Month Year': 'Month-Year', 'Minutes': 'Min & Sec)'},
                    )
        fig_1.update_layout(xaxis_tickformat='%b %Y', xaxis_title='Month-Year', yaxis_title='Min & Sec')

        data_range3 = 'O2:R9'
        df = worksheet.get_values(data_range3)
        df = pd.DataFrame(df[1:], columns=df[0])
        df.columns.values[0] = "Month Year"  

        df.rename(columns={"Month Year": "Month-Year"}, inplace=True)
        df[["New Users", "Existing Users", "Total Users"]] = df[["New Users", "Existing Users", "Total Users"]].astype(int)
        df_melted = df.melt(id_vars=["Month-Year"], value_vars=["New Users", "Existing Users", "Total Users"],
                            var_name="User Type", value_name="Count")

        fig_2 = px.bar(
            df_melted,
            x="Month-Year",
            y="Count",
            color="User Type",
            text="Count",
            labels={"Month-Year": "Month-Year", "Count": "User Count", "User Type": "Category"},
            height=500,
            barmode="stack"
        )

        fig_2.update_traces(texttemplate='%{text}', textposition='outside')
        fig_2.update_layout(xaxis=dict(type="category", tickmode="array", tickvals=df["Month-Year"]),
                            )

        data_range3 = 'O12:R19'
        df = worksheet.get_values(data_range3)
        df = pd.DataFrame(df[1:], columns=df[0])
        df.columns.values[0] = "Month Year"  

        df.rename(columns={"Month Year": "Month-Year"}, inplace=True)
        df[["New Teams", "Existing Teams", "Total Teams"]] = df[["New Teams", "Existing Teams", "Total Teams"]].astype(int)
        df_melted = df.melt(id_vars=["Month-Year"], value_vars=["New Teams", "Existing Teams", "Total Teams"],
                            var_name="Teams Type", value_name="Count")

        fig_3 = px.bar(
            df_melted,
            x="Month-Year",
            y="Count",
            color="Teams Type",
            text="Count",
            labels={"Month-Year": "Month-Year", "Count": "Teams Count", "Teams Type": "Category"},
            height=500,
            barmode="stack"
        )

        fig_3.update_traces(texttemplate='%{text}', textposition='outside')
        fig_3.update_layout(xaxis=dict(type="category", tickmode="array", tickvals=df["Month-Year"]),
                            )

        data_range3 = 'O22:R29'
        df = worksheet.get_values(data_range3)
        df = pd.DataFrame(df[1:], columns=df[0])
        df.columns.values[0] = "Month Year"  

        df.rename(columns={"Month Year": "Month-Year"}, inplace=True)
        df[["New Projects", "Existing Projects", "Total Projects"]] = df[["New Projects", "Existing Projects", "Total Projects"]].astype(int)
        df_melted = df.melt(id_vars=["Month-Year"], value_vars=["New Projects", "Existing Projects", "Total Projects"],
                            var_name="Projects Type", value_name="Count")

        fig_4 = px.bar(
            df_melted,
            x="Month-Year",
            y="Count",
            color="Projects Type",
            text="Count",
            labels={"Month-Year": "Month-Year", "Count": "Projects Count", "Projects Type": "Category"},
            height=500,
            barmode="stack"
        )

        fig_4.update_traces(texttemplate='%{text}', textposition='outside')
        fig_4.update_layout(xaxis=dict(type="category", tickmode="array", tickvals=df["Month-Year"]),
                            )

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Monthly Active Users")
            st.plotly_chart(fig)
        with col2:
            st.subheader("Avg Session Duration")
            st.plotly_chart(fig_1, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            st.subheader("Team Growth")
            st.plotly_chart(fig_2)

        with col4:
            st.subheader("Member Growth") 
            st.plotly_chart(fig_3)

        col5, col6 = st.columns(2)

        with col5:
            st.subheader("Project Growth")
            st.plotly_chart(fig_4)

        with col6:
            st.subheader("NPS Feedback")
            dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/NPS+Feedback(nKPI).png"
            st.image(dummy_image_url,  width=900)

    elif page == 'Knowledge':
        worksheet = sheet.get_worksheet(4)
        data_range2 = 'D1:G20'
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

            df2.sort_values(by="Month-Year", inplace=True)
            df2_long = df2.melt(
                id_vars="Month-Year",  
                value_vars=[col for col in df2.columns if col != "Month-Year"],  
                var_name="Type", 
                value_name="Value"
            )

            df2_long = df2_long[df2_long["Value"] > 0]

            df2_long["Month-Year"] = df2_long["Month-Year"].dt.strftime('%b %Y')  
            fig = px.bar(
                df2_long,
                x="Month-Year",
                y="Value",
                color="Type", 
                text="Value", 
                labels={"Month-Year": "Month-Year", "Value": "Office Hours"},
                height=500,
                barmode="stack"  
            )

            fig.update_traces(texttemplate='%{text}', textposition='outside')

            fig.update_layout(
                xaxis=dict(
                    type="category",  
                    tickmode="array",  
                    tickvals=df2_long["Month-Year"]                ),
                xaxis_title="Month-Year",
                yaxis_title="Office Hours",
                showlegend=True
            )

        try:
            worksheet = sheet.get_worksheet(4)

            data_range_stage = "T1:V20"
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

            df3_long = df3_long[df3_long["Count"] > 0]

            bar3 = px.bar(
                df3_long,
                x="Month Year",
                y="Count",
                text="Count",
                color="Type", 
                labels={"Month Year": "Month Year", "Count": "% Network Density"},
                barmode="group",  
                height=500
            )

            bar3.update_traces(texttemplate='%{text:.2%}', textposition='outside')

            bar3.update_layout(
                yaxis_tickformat='.0%', 
                xaxis_tickangle=360,  
            )
        except Exception as e:
            st.error(f"An error occurred: {e}")

        df = fetch_event_participation_member_data()

        if df is not None:
            df['month_year_datetime'] = pd.to_datetime(df['month_year'], format='%b %Y')
            df_pivot = df.pivot_table(index='month_year_datetime', columns='type', values='count', aggfunc='sum').reset_index()
            df_pivot = df_pivot.dropna(subset=['Host Count', 'Speaker Count', 'Attendee Count'], how='all')
            df_pivot = df_pivot.sort_values('month_year_datetime')
            sorted_months = df_pivot['month_year_datetime'].dt.strftime('%b %Y')
            df_melted = df_pivot.melt(id_vars=['month_year_datetime'], value_vars=['Host Count', 'Speaker Count', 'Attendee Count'],
                              var_name='type', value_name='count')
            df_melted['count'].fillna(0, inplace=True)
            fig_1 = px.bar(df_melted, x='month_year_datetime', y='count', color='type',
               labels={'month_year_datetime': 'Month-Year', 'count': 'Count', 'type': 'Type'},
               text='count',
               height=500)

            fig_1.update_traces(texttemplate='%{y}', textposition='outside')
            fig_1.update_layout(
                barmode='stack',
                xaxis=dict(
                    type='category',  
                    tickmode='array',  
                    tickvals=df_pivot['month_year_datetime'],  
                    ticktext=sorted_months  
                ),
                showlegend=True
            )
        else:
            st.warning("No data available")

        df = fetch_event_participation_team_data()

        if df is not None:
            df['month_year_datetime'] = pd.to_datetime(df['month_year'], format='%b %Y')
            df_pivot = df.pivot_table(index='month_year_datetime', columns='type', values='count', aggfunc='sum').reset_index()
            df_pivot = df_pivot.dropna(subset=['Host Count', 'Speaker Count', 'Attendee Count'], how='all')
            df_pivot = df_pivot.sort_values('month_year_datetime')
            sorted_months = df_pivot['month_year_datetime'].dt.strftime('%b %Y')
            df_melted = df_pivot.melt(id_vars=['month_year_datetime'], value_vars=['Host Count', 'Speaker Count', 'Attendee Count'],
                              var_name='type', value_name='count')
            fig_2 = px.bar(df_melted, x='month_year_datetime', y='count', color='type',
               labels={'month_year_datetime': 'Month-Year', 'count': 'Count', 'type': 'Type'},
               text='count',
               height=500)
            fig_2.update_traces(texttemplate='%{y}', textposition='outside')
            fig_2.update_layout(
                barmode='stack',
                xaxis=dict(
                    type='category',  
                    tickmode='array',  
                    tickvals=df_pivot['month_year_datetime'],  
                    ticktext=sorted_months  
                ),
                showlegend=True
            )
        else:
            st.warning("No data available")

        worksheet = sheet.get_worksheet(4)
        data_range_stage = "L1:O20"
        data2 = worksheet.get_values(data_range_stage)

        columns = ['Month Year', '# of hours of blog reading', '# of hours of workshops/problem solving', '# of hours of OHs']
        df = pd.DataFrame(data2[1:], columns=columns) 

        df['month_year_datetime'] = pd.to_datetime(df['Month Year'], errors='coerce')

        df_pivot = df.pivot_table(index='month_year_datetime', 
                                values=['# of hours of blog reading', '# of hours of workshops/problem solving', '# of hours of OHs'], 
                                aggfunc='sum').reset_index()

        df_pivot = df_pivot.dropna(subset=['# of hours of blog reading', '# of hours of workshops/problem solving', '# of hours of OHs'], how='all')
        df_pivot = df_pivot.sort_values('month_year_datetime')
        sorted_months = df_pivot['month_year_datetime'].dt.strftime('%b %Y')
        df_melted = df_pivot.melt(id_vars=['month_year_datetime'], 
                                value_vars=['# of hours of blog reading', '# of hours of workshops/problem solving', '# of hours of OHs'],
                                var_name='type', value_name='hours')

        df_melted['type'] = df_melted['type'].replace({
            '# of hours of blog reading': 'Blog Reading',
            '# of hours of workshops/problem solving': 'Workshop',
            '# of hours of OHs': 'OH'
        })

        bar2 = px.bar(df_melted, x='month_year_datetime', y='hours', color='type',
                    labels={'month_year_datetime': 'Month-Year', 'hours': 'Hours', 'type': 'Type'},
                    text='hours',
                    height=500)
        bar2.update_traces(texttemplate='%{y}', textposition='outside')

        bar2.update_layout(
            barmode='stack',
            xaxis=dict(
                type='category',  
                tickmode='array',  
                tickvals=df_pivot['month_year_datetime'],  
                ticktext=sorted_months  
            ),
            showlegend=True
        )

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Office Hours Held (By Type)")
            st.plotly_chart(fig)
        with col2:
            st.subheader("Hours of knowledge Contributed") 
            st.plotly_chart(bar2)

        col3, col4 = st.columns(2)

        with col3:
            st.subheader("% Network Density")
            st.plotly_chart(bar3)

        with col4:
            st.subheader("Monthly Active Users by Contribution Type - Events")
            st.plotly_chart(fig_1)

        col5, col6 = st.columns(2)

        with col5:
            st.subheader("Monthly Active Teams by Contribution Type - Events") 
            st.plotly_chart(fig_2)
    
    elif page == 'People/Talent':
        try:
            worksheet = sheet.get_worksheet(6)
            ranges = ['D1:E20', 'N1:O20', 'S1:W20']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)
            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                df1.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)
                df1['Value'] = df1['Value'].replace({',': '', ' ': ''}, regex=True)
                df1['Value'] = pd.to_numeric(df1['Value'], errors='coerce')
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)

                df1 = df1[df1['Value'] > 0]
                
                bar1 = px.bar(
                    df1,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Active People Count"},
                    height=500  
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside')
            
            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)
            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])
                df2.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df2.dropna(subset=["Month-Year", "Value"], inplace=True)
                df2['Value'] = df2['Value'].replace({',': '', ' ': ''}, regex=True)
                df2['Value'] = pd.to_numeric(df2['Value'], errors='coerce')
                df2.dropna(subset=["Month-Year", "Value"], inplace=True)

                df2 = df2[df2['Value'] > 0]
                bar2 = px.bar(
                    df2,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Network New Hires"},
                    height=500  
                )
                bar2.update_traces(texttemplate='%{text}', textposition='outside')

            data_range3 = ranges[2]
            data3 = worksheet.get_values(data_range3)
            if data3 and len(data3[0]) >= 2:
                df3 = pd.DataFrame(data3[1:], columns=data3[0])

                if "Month Year" in df3.columns:
                    df3.rename(columns={"Month Year": "Month-Year"}, inplace=True)

                for col in df3.columns:
                    if col != "Month-Year":
                        df3[col] = pd.to_numeric(df3[col], errors='coerce')

                df3.dropna(subset=["Month-Year"], inplace=True)

                df3["Month-Year"] = pd.to_datetime(df3["Month-Year"], errors="coerce")
                df3["Month-Year"] = df3["Month-Year"].dt.strftime('%b %Y')

                df3.sort_values(by="Month-Year", inplace=True)

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
                    color="Type", 
                    labels={"Month-Year": "Month-Year", "Value": "Monthly Talent"},
                    height=500,
                    barmode="stack"  
                )

                bar3.update_traces(texttemplate='%{text}', textposition='outside') 

                bar3.update_layout(
                    xaxis_tickformat="%b %Y", 
                    xaxis_tickangle=-45,  
                    xaxis_title="Month-Year",
                    yaxis_title="Monthly Talent",
                    legend_title="Team Level",
                    height=500,
                )
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("# of Active People in the Network")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Monthly New Hires into the Network")
                st.plotly_chart(bar2)

            col3, col4 = st.columns(2)

            with col3:
                st.subheader("Monthly Talent / Level Growth")
                st.plotly_chart(bar3)
        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'Projects':
        try:
            worksheet = sheet.get_worksheet(7)
            ranges = ['D1:E20', 'I1:M20']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)
            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                df1.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df1.columns:
                    df1["Value"] = pd.to_numeric(df1["Value"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df1.columns:
                    if col != "Month-Year":
                        df1[col] = pd.to_numeric(df1[col], errors='coerce')
                df1 = df1[df1["Value"] > 0]

                bar1 = px.bar(
                    df1,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Active People Count"},
                    height=500  
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside')


            data_range_stage = ranges[1]
            data = worksheet.get_values(data_range_stage)

            if data and len(data) > 1:  
                df3 = pd.DataFrame(data[1:], columns=data[0])

                if "Month Year" in df3.columns:
                    df3.rename(columns={"Month Year": "Month-Year"}, inplace=True)

                for col in ["Projects", "Stars", "Forks", "Repos"]:
                    if col in df3.columns:
                        df3[col] = pd.to_numeric(df3[col], errors='coerce')

                df3.dropna(subset=["Month-Year", "Projects", "Stars", "Forks", "Repos"], inplace=True)

                df3["Month-Year"] = pd.to_datetime(df3["Month-Year"], errors="coerce")
                df3["Month-Year"] = df3["Month-Year"].dt.strftime('%b %Y')

                df3_long = df3.melt(
                    id_vars=["Month-Year"], 
                    value_vars=["Projects", "Stars", "Forks", "Repos"],
                    var_name="Type", 
                    value_name="Count"
                )

                df3_long = df3_long[df3_long["Count"] > 0]

                bar3 = px.bar(
                    df3_long,
                    x="Month-Year",
                    y="Count",
                    text="Count",
                    color="Type",  
                    labels={"Month-Year": "Month-Year", "Count": "Value"},
                    barmode="group",  
                    height=500
                )

                bar3.update_traces(texttemplate='%{text}', textposition='outside')

                bar3.update_layout(
                    xaxis_tickangle=-45,  
                    xaxis_title="Month-Year",
                    yaxis_title="Value",
                    legend_title="Metric Type",
                    height=500,
                )


            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Project Contributors by Month")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Project Adoption:  Stars, Forks, and Repos")
                st.plotly_chart(bar3)

            # col3, col4 = st.columns(2)
        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'Programs':
        try:
            worksheet = sheet.get_worksheet(8)
            ranges = ['D1:E20', 'AF1:AG20']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)
            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                df1.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df1.columns:
                    df1["Value"] = pd.to_numeric(df1["Value"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df1.columns:
                    if col != "Month-Year":
                        df1[col] = pd.to_numeric(df1[col], errors='coerce')
                df1 = df1[df1["Value"] > 0]

                bar1 = px.bar(
                    df1,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Data"},
                    height=500  
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside')
            
            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)

            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])
                df2.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df2.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df2.columns:
                    df2["Value"] = df2["Value"].replace({'\$': '', ',': '', '': None}, regex=True)
                    df2["Value"] = pd.to_numeric(df2["Value"], errors='coerce')

                for col in df2.columns:
                    if col != "Month-Year":
                        df2[col] = pd.to_numeric(df2[col], errors='coerce')

                df2 = df2[df2["Value"] > 0]

                line_chart = px.line(
                    df2,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "Cost($)"},
                    height=500
                )

                line_chart.update_traces(
                    texttemplate='%{text}', 
                    textposition='top center', 
                    mode='lines+markers+text'  
                )

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Monthly Aggregated Program Impact Scores")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Program ROI (Imapct vs Cost)")
                st.plotly_chart(line_chart)

            # col3, col4 = st.columns(2)
        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'Service Providers':
        try:
            worksheet = sheet.get_worksheet(9)
            ranges = ['I1:J20', 'N1:O20']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)
            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                df1.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df1.columns:
                    df1["Value"] = pd.to_numeric(df1["Value"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df1.columns:
                    if col != "Month-Year":
                        df1[col] = pd.to_numeric(df1[col], errors='coerce')

                df1 = df1[df1["Value"] > 0]

                bar1 = px.bar(
                    df1,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. of Service Providers"},
                    height=500  
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside')
            
            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)
            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])
                df2.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df2.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df2.columns:
                    df2["Value"] = pd.to_numeric(df2["Value"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df2.columns:
                    if col != "Month-Year":
                        df2[col] = pd.to_numeric(df2[col], errors='coerce')
                df2 = df2[df2["Value"] > 0]

                bar2 = px.bar(
                    df2,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. of Service Providers"},
                    height=500  
                )
                bar2.update_traces(texttemplate='%{text}', textposition='outside')

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Service Providers: Listed on Network Tools")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Service Providers:  Match within 6 months")
                st.plotly_chart(bar2)

            # col3, col4 = st.columns(2)
        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'Other Networks':
        try:
            worksheet = sheet.get_worksheet(10)
            ranges = ['D1:E20', 'I1:J20']

            data_range1 = ranges[0]
            data1 = worksheet.get_values(data_range1)
            if data1 and len(data1[0]) >= 2:
                df1 = pd.DataFrame(data1[1:], columns=data1[0])
                df1.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df1.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df1.columns:
                    df1["Value"] = pd.to_numeric(df1["Value"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df1.columns:
                    if col != "Month-Year":
                        df1[col] = pd.to_numeric(df1[col], errors='coerce')

                df1 = df1[df1["Value"] > 0]

                bar1 = px.bar(
                    df1,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. of Network"},
                    height=500  
                )
                bar1.update_traces(texttemplate='%{text}', textposition='outside')
            
            data_range2 = ranges[1]
            data2 = worksheet.get_values(data_range2)
            if data2 and len(data2[0]) >= 2:
                df2 = pd.DataFrame(data2[1:], columns=data2[0])
                df2.rename(columns={"Month Year": "Month-Year", "Data": "Value"}, inplace=True)
                df2.dropna(subset=["Month-Year", "Value"], inplace=True)

                if "Value" in df2.columns:
                    df2["Value"] = pd.to_numeric(df2["Value"].replace({',': '', '': None}).apply(lambda x: float(x) if x else None), errors='coerce')

                for col in df2.columns:
                    if col != "Month-Year":
                        df2[col] = pd.to_numeric(df2[col], errors='coerce')
                df2 = df2[df2["Value"] > 0]


                bar2 = px.bar(
                    df2,
                    x="Month-Year",
                    y="Value",
                    text="Value",
                    labels={"Month-Year": "Month-Year", "Value": "No. of Network"},
                    height=500  
                )
                bar2.update_traces(texttemplate='%{text}', textposition='outside')

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Networks Engaged with PL")
                st.plotly_chart(bar1)

            with col2:
                st.subheader("Networks Building/Participating with PL Programs")
                st.plotly_chart(bar2)

        except Exception as e:
            st.error(f"An error occurred: {e}")

    elif page == 'User/Customers':
        st.subheader("")

if __name__ == "__main__":
    main()

   