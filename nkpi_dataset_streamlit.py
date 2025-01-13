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

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Team Growth")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Member Growth") 

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Project Growth")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

        st.subheader("NPS Feedback")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

    elif page == 'Knowledge':
        st.subheader("Office Hours Held (By Type)")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)
       
        st.subheader("% Network Density")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Network+Density(nKPI).png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Monthly Active Users by Contribution Type - Events")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

        st.subheader("Monthly Active Teams by Contribution Type - Events")

        dummy_image_url = "https://plabs-assets.s3.us-west-1.amazonaws.com/Coming+Soon.png"
        st.image(dummy_image_url,  width=900)

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

   