import streamlit as st
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from urllib.parse import quote_plus
import datetime, pandas as pd
import traceback
import altair as alt
import pytz, random
from datetime import timedelta

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

@st.cache_data(ttl=1*24*60*60) # 1 day  
def get_live_status(curr_timestamp):
    curr_date = curr_timestamp.date().strftime("%B %d, %Y")
    curr_time = curr_timestamp.strftime("%X")
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
        curr_data = f"""
                        - **Date:** {curr_date}
                        - **Time:** {curr_time}
                        - **Status:** Running
                        - **StoppedTime:** NA
                    """
        st.success(curr_data)
    else:
        curr_data = f"""
                        - **Date:** {curr_date}
                        - **Time:** {curr_time}
                        - **Status:** Stopped
                        - **StoppedTime:** {StopTime}
                    """
        st.error(curr_data)

@st.cache_data(ttl=1*24*60*60) # 1 day
def get_downtime_by_parameter(parameter,today_date):
    # print(f"Getting records till {today_date}")
    # print()
    curr_qry = f"""with s1 as
                   (
                     select date(stop_date) as stop_date, coalesce({parameter},'NA') as {parameter}, start_date - stop_date as duration 
                     from LineStopSchema.production_line_table plt 
                     where start_date is not null and stop_date::date > (current_date - interval '6 months')::date and {parameter} <> 'NA'
                   )
                   select stop_date,{parameter},SUM(duration) as Duration
                   from s1
                   group by GROUPING sets ((stop_date, {parameter}),(stop_date))
                   order by stop_date desc, {parameter}"""
    # print("Query:")
    # print(curr_qry)
    # print()
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

def format_cause_data(db_res_dict,start_date,stop_date):
    cause_dict = {}
    for stp_date, details_dict in db_res_dict.items():
        if stp_date>=start_date and stp_date<=stop_date:
            for cause,dur in details_dict.items():
                cause = cause.strip().title()
                if (cause.lower() != "all") and not(cause.lower().startswith('test')) and ('na' != cause.lower()):
                    if not(cause in cause_dict):
                        cause_dict[cause] = dur
                    else:
                        cause_dict[cause] = cause_dict[cause]+dur
    resp_dict = {'Cause':[],'Duration':[]}
    for cause,dur in cause_dict.items():
        resp_dict['Cause'].append(cause)
        resp_dict['Duration'].append(dur)
    resp_df = pd.DataFrame.from_dict(resp_dict).sort_values(by='Duration',ascending=False).head(10).reset_index(drop=True)
    resp_df['Duration'] = (resp_df['Duration']/60).astype(int)
    return resp_df
@st.cache_data(ttl=1*24*60*60)
def get_todays_status(curr_timestamp):
    curr_timestamp=curr_timestamp#+ datetime.timedelta(hours=8)
    start_timestamp = curr_timestamp.replace(hour=6, minute=0, second=0)
    target_timestamp = curr_timestamp.replace(hour=16, minute=0, second=0)
    if curr_timestamp > start_timestamp:
        curr_query = '''SET TIMEZONE TO 'America/Chicago';
                        with stop_records as
                        (
                            select line_stop_id,start_date - stop_date as duration from LineStopSchema.production_line_table 
                            where  start_date is not null and stop_date::date  = current_date
                            union
                            select line_stop_id, date_trunc('second',(now()::timestamp - stop_date)) as duration from LineStopSchema.production_line_table 
                            where  start_date is null and stop_date::date  = current_date
                            union 
                            select line_stop_id, date_trunc('second',(now()::timestamp - CURRENT_DATE)) as duration from LineStopSchema.production_line_table 
                            where  start_date is null and stop_date::date  < current_date 
                            union
                            select line_stop_id,date_trunc('second',(start_date - CURRENT_DATE)) as duration from LineStopSchema.production_line_table 
                            where start_date::date = current_date::date and stop_date::date = (current_date - interval '1 day')::date
                        )
                        SELECT COALESCE(CAST(sum(duration) as varchar),'00:00:00') AS total_duration from stop_records'''
        curr_values = {}
        db_res = str(run_query(curr_query,curr_values)[0][0])
        stop_duration = datetime.datetime.strptime(db_res,"%H:%M:%S")
        stop_duration_str = stop_duration.strftime("%-H hours %-M minutes %-S seconds")
        if curr_timestamp > target_timestamp:
            line_timestamp = target_timestamp
        else:
            line_timestamp = curr_timestamp
        total_runtime = line_timestamp - start_timestamp
        total_runtime = datetime.datetime.strptime(str(total_runtime), "%H:%M:%S")
        start_duration_str = total_runtime.strftime("%-H hours %-M minutes %-S seconds")
        total_runtime_mins = total_runtime.hour*60+total_runtime.minute
        peice_count = total_runtime_mins*5
        peice_count_in = random.randint(peice_count-15,peice_count+15)
        peice_count_out = random.randint(peice_count-15,peice_count+15)
        avg_speed = f"{round(peice_count_out/total_runtime_mins,2)} frames per minute"
        
    else:
        start_duration_str = "Production Line not yet started."
        stop_duration_str = "Production Line not yet started."
        avg_speed = 'NA'
        peice_count_in = 0
        peice_count_out = 0
    return dict(uptime=start_duration_str,downtime=stop_duration_str,pin=peice_count_in,pout=peice_count_out,speed=avg_speed)
    
    
curr_timestamp = datetime.datetime.now(pytz.timezone('America/Chicago'))

st.set_page_config(
    page_title="Dunbarton",
    page_icon="🚪",
    layout="wide",
    initial_sidebar_state="expanded")

MainHeader = st.header('Production Line Reports', divider='rainbow')

st.subheader(body = 'Live Status', anchor='#paint-line-reports')
get_live_status(curr_timestamp)
st.divider()


curr_date = curr_timestamp.date()
st.subheader(body = "Today's Status", anchor='#paint-line-reports')
status_dict = get_todays_status(curr_timestamp)
col1, col2, col3 = st.columns(3)
with col1:
    st.info(f'**Date:** {curr_timestamp.date().strftime("%B %d, %Y")}')
with col2:
    st.info(f'**Time:** {curr_timestamp.strftime("%I:%M %p")}')
with col3:
    st.info(f"**Start Time:** 06:00 AM")
col1, col2 = st.columns(2)
with col1:
    st.info(f'**Total Runtime:** {status_dict["uptime"]}')
with col2:
    st.info(f'**Total Stoptime:** {status_dict["downtime"]}')
col1, col2, col3 = st.columns(3)
with col1:
    st.info(f'**Total Peice Count IN:** {status_dict["pin"]}')
with col2:
    st.info(f'**Total Peice Count OUT:** {status_dict["pout"]}')
with col3:
    st.info(f"**Average Speed:** {status_dict['speed']}")
st.divider()


st.subheader(body = 'Production Line Downtime By Category ID', anchor='#paint-line-reports')
all_dates,all_parameters,db_res_dict = get_downtime_by_parameter("category_id",curr_date)
col1, col2, col3 = st.columns(3)
with col1:
    cat_id = st.selectbox(label ="Category ID:",options=all_parameters,index=0)
with col2:
    cat_id_start_date = st.date_input(label="Start Date:", value=min(all_dates), min_value=min(all_dates), max_value=all_dates[-2])
with col3:
    cat_id_stop_date = st.date_input(label="Stop Date:", value = max(all_dates), min_value=all_dates[all_dates.index(cat_id_start_date)+1], max_value=max(all_dates))
    
CatID_DF = format_data(all_dates,all_parameters,db_res_dict,cat_id_start_date,cat_id_stop_date)

but_col1, but_col1, but_col3 = st.columns([3, 2, 1])
with but_col3:
    st.download_button(
    label="Download data as CSV",
    data=convert_df_to_csv(CatID_DF),
    file_name='Report.csv',
    mime='text/csv',
    )

chart = alt.Chart(CatID_DF).mark_bar().encode(
            x=alt.X('Date:O', axis=alt.Axis(labelOverlap="greedy",grid=False,labelAngle=-45,)),
            y=alt.Y(cat_id,title="Down Time (Mins)"))
st.altair_chart(chart, use_container_width=True)
st.divider()


add_cat_all_dates,add_cat_all_parameters,add_cat_db_res_dict = get_downtime_by_parameter("additional_category",curr_date)
st.subheader(body = 'Production Line Downtime By Additonal Category ID', anchor='#paint-line-reports')

col1, col2, col3 = st.columns(3)
with col1:
    add_cat_id = st.selectbox(label ="Additional Category:",options=add_cat_all_parameters,index=0)
with col2:
    add_cat_id_start_date = st.date_input(label="Start Date:", value=min(add_cat_all_dates), min_value=min(add_cat_all_dates), max_value=add_cat_all_dates[-2], key='add_cat_start_date')
with col3:
    add_cat_id_stop_date = st.date_input(label="Stop Date:", value = max(add_cat_all_dates), min_value=add_cat_all_dates[add_cat_all_dates.index(add_cat_id_start_date)+1], max_value=max(add_cat_all_dates), key='add_cat_stop_date')
    
AddCatID_DF = format_data(add_cat_all_dates,add_cat_all_parameters,add_cat_db_res_dict,add_cat_id_start_date,add_cat_id_stop_date)
# print(AddCatID_DF)

add_cat_chart = alt.Chart(AddCatID_DF).mark_bar().encode(
                x=alt.X('Date:O', axis=alt.Axis(labelOverlap="greedy",grid=False,labelAngle=-45,)),
                y=alt.Y(add_cat_id,title="Down Time (Mins)"))
st.altair_chart(add_cat_chart, use_container_width=True)
st.divider()



all_dates,all_parameters,db_res_dict = get_downtime_by_parameter("cause",curr_date)
st.subheader(body = 'Production Line Downtime By Causes (Top 10)', anchor='#paint-line-reports')

col1, col2 = st.columns(2)
with col1:
    cause_start_date = st.date_input(label="Start Date:", value=min(all_dates), min_value=min(all_dates), max_value=all_dates[-2], key='cause_start_date')
with col2:
    cause_stop_date = st.date_input(label="Stop Date:", value = max(all_dates), min_value=all_dates[all_dates.index(cause_start_date)+1], max_value=max(all_dates), key='cause_stop_date')
    
CauseDF = format_cause_data(db_res_dict,cause_start_date,cause_stop_date)

chart = alt.Chart(CauseDF).mark_bar().encode(
            x=alt.X('Cause', axis=alt.Axis(labelOverlap="greedy",grid=False,labelAngle=-45),sort=None),
            y=alt.Y('Duration',title="Down Time (Mins)"))
st.altair_chart(chart, use_container_width=True)
st.divider()


