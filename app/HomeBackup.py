import streamlit as st

st.set_page_config(
    page_title="Dunbarton",
    page_icon="🚪",
    layout="wide",
    initial_sidebar_state="expanded")

st.sidebar.success("Select a section above.")
st.write("# Welcome to Dunbarton Reporting App! 👋")
st.markdown(
        """
        👈 **Access a wide variety of reporting charts by selecting the sections on the left.**

        ### Want to learn more about your options?

        - Paint Line Reports - Get access to realtime Paint Line informations.

        ### Have a question

        - Drop an email to **johns@dunbarton.com**
    """
    )

