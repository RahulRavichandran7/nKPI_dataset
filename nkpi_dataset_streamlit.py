import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from pyvis.network import Network
import streamlit.components.v1 as components
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import calendar
import numpy as np
import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
load_dotenv()

def get_database_connection():
    DATABASE_URL = os.getenv("DB_URL")
    engine = create_engine(DATABASE_URL)
    return engine


def main():
    st.set_page_config(page_title="nKPI Dashboard", layout="wide")

    page = st.sidebar.radio(
        "nKPI Dashboard",
        [
           "Activity - Directory Teams",
            "Office Hours Usage",
            "Directory MAUs",
            "Network Density",
            "IRL Gatherings",
            "Network Growth",
            # "Hackathons",
            "Usage Activity"
        ]
    )

    engine = get_database_connection()

    if page == "Activity - Directory Teams":
        st.title("Activity -- Directory Teams")

        st.markdown("""Breakdown of user engagement and activity on Directory team profiles""")

        


    elif page == "Office Hours Usage":
        st.title("Office Hours Usage")
        
        st.markdown("""
            Breakdown of OH activity on Member and Team Profile
        """)

        

    elif page == "Directory MAUs":
        st.title("Directory MAUs")

        df = fetch_average_session_time(engine)
        avg_minutes = int(df['average_duration_minutes'][0])
        avg_seconds = int(df['average_duration_seconds'][0])

        # Display the results
        # st.header("Session Duration Summary")
        # st.metric("Average Session Duration", f"{avg_minutes} min {avg_seconds} sec")

        st.markdown(
            f"""
            <div style="background-color:#FFD700; padding:20px; border-radius:8px; text-align:center;">
                <h3>Average Session Duration</h3>
                <p style="font-size:28px; font-weight:bold; color:#2b2b2b;">{avg_minutes} min {avg_seconds} sec</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    elif page == 'Network Density':
        st.title("Network Density")

    elif page == 'Network Strength - LongTerm Connectivity':
        st.title("Network Strength - LongTerm Connectivity")

    elif page == 'Network Strength - Conversation Based Connectivity':
        st.title("Activity --  Network Strength - Conversation Based Connectivity")
       
    elif page == 'IRL Gatherings':
        st.title("IRL Gatherings")


if __name__ == "__main__":
    main()