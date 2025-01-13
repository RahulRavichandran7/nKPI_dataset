import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
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
    engine = get_database_connection()
    return pd.read_sql(query, engine)

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
    engine = get_database_connection()
    return pd.read_sql(query, engine)

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
    engine = get_database_connection()
    return pd.read_sql(query, engine)


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
    engine = get_database_connection()
    return pd.read_sql(query, engine)



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
    engine = get_database_connection()
    return pd.read_sql(query, engine)


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
    engine = get_database_connection()
    return pd.read_sql(query, engine)




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

    engine = get_database_connection()

    if page == "Capital":
        st.subheader("Capital Raised by PL Portfolio Venture Startups")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Capital+Raised+PL(nKPI).png"
        st.image(dummy_image_url,  width=900)
       
        st.subheader("Capital Raised by All Organizations in the Network")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Capital+Raised+by+Org(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Angel Investors of Network Teams")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Angel+Investors+of+Network(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("VC Investors of Network Teams")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/VC+Investors+of+Teams(nKPI).png"
        st.image(dummy_image_url,  width=900)

    elif page == "Teams":
        st.subheader("Shut down, Same stage and Moved up")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Shut+down%2C+moved+team(nKPI).png"
        st.image(dummy_image_url,  width=900)
       
        st.subheader("Teams by Membership Tier")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Teams+by+Membership(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Teams by Impact Tier")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Teams+by+Impact+Tier(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("% of Top Teams in each focus area that are engaged with Protocol Labs")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)
        
    elif page == "Brand":
        st.subheader("Share of Voice")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/share+of+voice(nKPI).png"
        st.image(dummy_image_url,  width=900)
       
        st.subheader("Audience Growth")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Audience+Growth(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Engagement Rate")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Engagement+Rate(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Email Subscribers")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Email+Subscribers(nKPI).png"
        st.image(dummy_image_url,  width=900)

    elif page == 'Network Tooling':
        st.subheader("Monthly Active Users")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)
       
        st.subheader("Avg Session Duration")

        # dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        # st.image(dummy_image_url,  width=900)

        session_data = fetch_session_durations()

        if not session_data.empty:
            # Combine minutes and seconds into a single value
            session_data["average_duration_combined"] = (
                session_data["average_duration_minutes"] + session_data["average_duration_seconds"] / 60
            )

            # Convert year and month into a proper datetime column for easier handling
            session_data["year_month"] = pd.to_datetime(session_data[["year", "month"]].assign(day=1))

            # Create an interactive Plotly line chart
            fig = px.line(
                session_data,
                x="year_month",
                y="average_duration_combined",
                # title="Monthly Average Session Duration",
                labels={
                    "year_month": "Year-Month",
                    "average_duration_combined": "Avg Duration (minutes.seconds)"
                },
                markers=True
            )

            # Format the x-axis to show 'Jan 2024' style labels
            fig.update_xaxes(
                tickformat="%b %Y",  # Display as 'Jan 2024'
                title_text="Year-Month"
            )

            # Display the chart in Streamlit
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning("No data available for session durations.")

        st.subheader("Team Growth")

        df = fetch_team_data()

        df_long = df.melt(
            id_vars=["month_year"],  # Columns to keep as is
            value_vars=["new_entries", "existing_entries", "total_entries"],  # Columns to unpivot
            var_name="type",  # New column name for variable names
            value_name="count"  # New column name for values
        )

        # Rename the values in the 'type' column for better readability
        type_mapping = {
            "new_entries": "New Entries",
            "existing_entries": "Existing Entries",
            "total_entries": "Total Entries"
        }
        df_long["type"] = df_long["type"].replace(type_mapping)

        # Plot the stacked bar chart
        fig = px.bar(
            df_long, 
            x="month_year",  # X-axis is 'month_year'
            y="count",  # Y-axis is the count column
            color="type",  # Color by the 'type' column
            labels={"month_year": "Month-Year", "count": "Number of Entries", "type": "Entry Type"},  # Updated labels
            text_auto=True  # Display the count on top of the bars
        )

        # Customize layout for better presentation
        fig.update_layout(
            barmode="stack",  # Stack the bars
            xaxis_title="Month-Year",
            yaxis_title="Number of Entries",
            legend_title="Entry Type",  # Update legend title
            showlegend=True
        )

        # Display the plot in Streamlit
        st.plotly_chart(fig)

        st.subheader("Member Growth") 

        df = fetch_member_data()

        df_long = df.melt(
            id_vars=["month_year"],  # Columns to keep as is
            value_vars=["new_entries", "existing_entries", "total_entries"],  # Columns to unpivot
            var_name="type",  # New column name for variable names
            value_name="count"  # New column name for values
        )

        # Rename the values in the 'type' column for better readability
        type_mapping = {
            "new_entries": "New Entries",
            "existing_entries": "Existing Entries",
            "total_entries": "Total Entries"
        }
        df_long["type"] = df_long["type"].replace(type_mapping)

        # Plot the stacked bar chart
        fig = px.bar(
            df_long, 
            x="month_year",  # X-axis is 'month_year'
            y="count",  # Y-axis is the count column
            color="type",  # Color by the 'type' column
            labels={"month_year": "Month-Year", "count": "Number of Entries", "type": "Entry Type"},  # Updated labels
            text_auto=True  # Display the count on top of the bars
        )

        # Customize layout for better presentation
        fig.update_layout(
            barmode="stack",  # Stack the bars
            xaxis_title="Month-Year",
            yaxis_title="Number of Entries",
            legend_title="Entry Type",  # Update legend title
            showlegend=True
        )

        # Display the plot in Streamlit
        st.plotly_chart(fig)

        st.subheader("Project Growth")

        df = fetch_project_data()

        df_long = df.melt(
            id_vars=["month_year"],  # Columns to keep as is
            value_vars=["new_entries", "existing_entries", "total_entries"],  # Columns to unpivot
            var_name="type",  # New column name for variable names
            value_name="count"  # New column name for values
        )

        # Rename the values in the 'type' column for better readability
        type_mapping = {
            "new_entries": "New Entries",
            "existing_entries": "Existing Entries",
            "total_entries": "Total Entries"
        }
        df_long["type"] = df_long["type"].replace(type_mapping)

        # Plot the stacked bar chart
        fig = px.bar(
            df_long, 
            x="month_year",  # X-axis is 'month_year'
            y="count",  # Y-axis is the count column
            color="type",  # Color by the 'type' column
            labels={"month_year": "Month-Year", "count": "Number of Entries", "type": "Entry Type"},  # Updated labels
            text_auto=True  # Display the count on top of the bars
        )

        # Customize layout for better presentation
        fig.update_layout(
            barmode="stack",  # Stack the bars
            xaxis_title="Month-Year",
            yaxis_title="Number of Entries",
            legend_title="Entry Type",  # Update legend title
            showlegend=True
        )

        # Display the plot in Streamlit
        st.plotly_chart(fig)

        st.subheader("NPS Feedback")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/NPS+Feedback(nKPI).png"
        st.image(dummy_image_url,  width=900)

    elif page == 'Knowledge':
        st.subheader("Office Hours Held (By Type)")

        df = fetch_OH_data()

        if not df.empty:
            df_pivot = df.pivot_table(index="month_year", columns="page_type", values="interaction_count", aggfunc="sum").fillna(0)

            fig = px.bar(df_pivot,
                        x=df_pivot.index,  
                        y=df_pivot.columns,  
                        labels={"value": "Interaction Count", "month_year": "Month-Year", "page_type":"Page Type"},
                        height=400)

            fig.update_layout(barmode='stack', xaxis_tickangle=-45)
            st.plotly_chart(fig)

        else:
            st.warning("No data available to display.")
       
        st.subheader("% Network Density")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Network+Density(nKPI).png"
        st.image(dummy_image_url,  width=900)

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

            df_pivot = df.pivot_table(index="month_year", 
                                    values=["Host Count", "Speaker Count", "Attendee Count"], 
                                    aggfunc="sum").fillna(0)

            df_pivot = df_pivot.reset_index()  
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
                labels={"Count": "Member Count", "month_year": "Month-Year", "Type": "Participant Type"},
                height=400
            )

            fig.update_layout(
                barmode='stack',  
                xaxis_tickangle=-45,  
                legend_title="Type"  
            )

            st.plotly_chart(fig)

        else:
            st.warning("No data available to display.")

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

            df_pivot = df.pivot_table(index="month_year", 
                                    values=["Host Count", "Speaker Count", "Attendee Count"], 
                                    aggfunc="sum").fillna(0)

            df_pivot = df_pivot.reset_index()  
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
                xaxis_tickangle=-45, 
                legend_title="Type"  
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

   