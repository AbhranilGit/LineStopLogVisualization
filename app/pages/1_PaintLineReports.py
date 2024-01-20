import streamlit as st
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from urllib.parse import quote_plus
import datetime, pandas as pd
import traceback
import altair as alt

@contextmanager
def db_session():
    db_url = '172.21.10.224'
    db_port = '5432'
    db_user = 'postgres'
    db_password = 'Dunbarton@123'
    db_name = 'postgres'
    db_conn_string = f'postgresql+psycopg2://{db_user}:%s@{db_url},{db_port}/{db_name}' % quote_plus(db_password)
    # print(db_conn_string)
    engine = create_engine(db_conn_string)
    connection = engine.connect()
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=engine))
    yield db_session
    db_session.close()
    connection.close()
    
def run_query(query,values):
    # print(values)
    result = None
    try:
        with db_session() as db:
            # print(text(query).bindparams(values).compile(compile_kwargs={"literal_binds": True}))
            result = db.execute(text(query),values).fetchall()
            db.commit()
    except Exception as e:
        print()
        print(traceback.format_exc())
        print()
    return result

def display_current_status():
    Status = 'Running'
    StopTime = 'NA'
    curr_qry = "SELECT line_stop_id,stop_date FROM LineStopSchema.production_line_table WHERE start_date IS null ORDER BY start_date desc "
    curr_values = {}
    db_res = run_query(curr_qry,curr_values)
    if db_res is not None:
        if len(db_res) > 0:
            Status = 'Stopped'
            StopTime = db_res[0][1].replace(tzinfo=None)
            StopTimeSplitted = str(StopTime).split()
            StopTimeDateSplitted = StopTimeSplitted[0].split("-")
            StopTime = StopTimeDateSplitted[1]+"-"+StopTimeDateSplitted[-1]+"-"+StopTimeDateSplitted[0]+" "+StopTimeSplitted[-1]
    # Status = "Running"
    if Status == "Running":
        curr_data = """
                        - **Status:** Running
                        - **StoppedTime:** NA
                    """
        st.success(curr_data)
    else:
        curr_data = f"""
                        - **Status:** Stopped
                        - **StoppedTime:** {StopTime}
                    """
        st.error(curr_data)

@st.cache_data(ttl=1*24*60*60) # 1 day
def get_downtime_by_parameter(parameter,today_date):
    print(f"Getting records till {today_date}")
    print()
    curr_qry = f"""with s1 as
                   (
                     select date(stop_date) as stop_date, {parameter}, start_date - stop_date as duration 
                     from LineStopSchema.production_line_table plt 
                     where start_date is not null and stop_date::date > (current_date - interval '6 months')::date
                   )
                   select stop_date,{parameter},SUM(duration) as Duration
                   from s1
                   group by GROUPING sets ((stop_date, {parameter}),(stop_date))
                   order by stop_date desc, {parameter}"""
    curr_values = {}
    db_res = run_query(curr_qry,curr_values)
    db_res_dict = {}
    all_parameters = []
    for row in db_res:
        if not(row[0] in db_res_dict):
            db_res_dict[row[0]] = {}
            curr_dict = db_res_dict[row[0]]
        else:
            curr_dict = db_res_dict[row[0]]
        if row[1] is not None:
            curr_dict[row[1].title()] =  row[2].seconds
            if not(row[1].title() in all_parameters):
                all_parameters.append(row[1].title())
        else:
            curr_dict['All'] =  row[2].seconds
            if not('All' in all_parameters):
                all_parameters.append('All')
    all_parameters = sorted(all_parameters)
    # print(db_res_dict)
    all_dates = sorted(db_res_dict.keys())
    return (all_dates,all_parameters,db_res_dict)

@st.cache_data(ttl=1*24*60*60) # 1 day  
def format_data(all_dates,all_parameters,db_res_dict,start_date,stop_date):
    filtered_dates = [item for item in all_dates if ((item>=start_date) and (item<=stop_date))]
    df_dict = {'Date':all_parameters}
    for item in filtered_dates:
       df_dict[item] = []
    for curr_dt in filtered_dates:
        for param in all_parameters:
            if param in db_res_dict[curr_dt]:
                df_dict[curr_dt].append(db_res_dict[curr_dt][param])
            else:
                df_dict[curr_dt].append(0)
    curr_df = pd.DataFrame.from_dict(df_dict)
    curr_df['Total'] = curr_df.sum(axis=1, numeric_only=True)
    curr_df = curr_df[curr_df.Total != 0].reset_index(drop=True)
    curr_df = curr_df.drop(['Total'],axis=1)
    curr_df = curr_df.T
    curr_df.columns = curr_df.iloc[0]
    curr_df = curr_df[1:]
    curr_df = curr_df.reset_index(drop=False).rename(columns={'index':'Date'}).reset_index(drop=True).rename_axis(None, axis=1)
    curr_df['Date'] = pd.to_datetime(curr_df['Date'])
    curr_df['Date'] = curr_df['Date'].dt.strftime('%m-%d-%Y')
    cat_columns = [col for col in curr_df.columns if not(col=='Date')]
    for col in cat_columns:
        curr_df[col] = (curr_df[col]/60).astype(int)
    return curr_df

@st.cache_data(ttl=1*24*60*60) # 1 day  
def convert_df_to_csv(df):
  # IMPORTANT: Cache the conversion to prevent computation on every rerun
  return df.to_csv(index=False).encode('utf-8')

st.set_page_config(
    page_title="Dunbarton",
    page_icon="🚪",
    layout="wide",
    initial_sidebar_state="expanded")

MainHeader = st.header('Paint Line Reports', divider='rainbow')

st.subheader(body = 'Live Status', anchor='#paint-line-reports')
display_current_status()
st.divider()
curr_date = datetime.datetime.today().date()
all_dates,all_parameters,db_res_dict = get_downtime_by_parameter("category_id",curr_date)

st.subheader(body = 'Paint Line Downtime By Category ID', anchor='#paint-line-reports')

col1, col2, col3 = st.columns(3)
with col1:
    cat_id = st.selectbox(label ="Category ID:",options=all_parameters,index=0)
with col2:
    start_date = st.date_input(label="Start Date:", value=min(all_dates), min_value=min(all_dates), max_value=all_dates[-2])
with col3:
    stop_date = st.date_input(label="Stop Date:", value = max(all_dates), min_value=all_dates[all_dates.index(start_date)+1], max_value=max(all_dates))
    
DF = format_data(all_dates,all_parameters,db_res_dict,start_date,stop_date)

but_col1, but_col1, but_col3 = st.columns([3, 2, 1])
with but_col3:
    st.download_button(
    label="Download data as CSV",
    data=convert_df_to_csv(DF),
    file_name='Report.csv',
    mime='text/csv',
    )

chart = alt.Chart(DF).mark_bar().encode(
            x=alt.X('Date:O', axis=alt.Axis(labelOverlap="greedy",grid=False,labelAngle=-45,)),
            y=alt.Y(cat_id,title="Down Time (Mins)"))
st.altair_chart(chart, use_container_width=True)
st.divider()



