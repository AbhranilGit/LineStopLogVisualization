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

st.set_page_config(
        page_title="Dunbarton",
        page_icon="🚪",
        layout="wide",
        initial_sidebar_state="expanded")

def create_db_engine():
    db_url = 'plc-database.c3ggsmikm5db.us-east-2.rds.amazonaws.com'
    db_port = '5432'
    db_user = 'dunbarton'
    db_password = 'DBM3tric!!'
    db_name = 'postgres'
    db_conn_string = f'postgresql+psycopg2://{db_user}:%s@{db_url},{db_port}/{db_name}' % quote_plus(db_password)
    engine = create_engine(db_conn_string)
    return engine

@contextmanager
def db_session(engine):
    connection = engine.connect()
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=engine))
    yield db_session
    db_session.close()
    connection.close()


def run_query(engine,query,values):
    # print(values)
    result = None
    try:
        with db_session(engine) as db:
            # print(text(query).bindparams(values).compile(compile_kwargs={"literal_binds": True}))
            result = db.execute(text(query),values).fetchall()
            db.commit()
    except Exception as e:
        print()
        print(traceback.format_exc())
        print()
    return result

def fetch_report_data():
    curr_engine = None
    report_data_dict = dict(date=[],
                            start_time=[],
                            stop_time=[],
                            up_duration=[],
                            down_duration=[],
                            last_active_time=[],
                            inactive_duration=[],
                            non_productive_hours=[],
                            avg_piece_count=[],
                            avg_speed=[])
    try:
        curr_timestamp = datetime.datetime.now(pytz.timezone('America/Chicago'))
        curr_engine = create_db_engine()
        if curr_timestamp.time() > datetime.time(18):
            end_date = curr_timestamp.date()
            start_date = end_date - timedelta(days=14)
        else:
            end_date = curr_timestamp.date() - timedelta(days=1)
            start_date = end_date - timedelta(days=14)
        curr_query = f"""SELECT report_date,start_time,stop_time,up_duration,down_duration,last_active_time,inactive_duration,non_productive_hours,avg_piece_count,avg_speed 
        FROM plc.reports WHERE report_date between :report_start_date AND :report_end_date"""
        curr_values = dict(report_start_date=str(start_date),report_end_date=str(end_date))
        curr_query_res = run_query(curr_engine,curr_query,curr_values)
        if curr_query_res is not None:
            for row in curr_query_res:
                report_data_dict['date'].append(row[0])
                report_data_dict['start_time'].append(row[1])
                report_data_dict['stop_time'].append(row[2])
                report_data_dict['up_duration'].append(row[3])
                report_data_dict['down_duration'].append(row[4])
                report_data_dict['last_active_time'].append(row[5])
                report_data_dict['inactive_duration'].append(row[6])
                report_data_dict['non_productive_hours'].append(row[7])
                report_data_dict['avg_piece_count'].append(row[8])
                report_data_dict['avg_speed'].append(row[9])
        if not(end_date in report_data_dict['date']):
            curr_query=f"""WITH time_check AS 
                        (
                            SELECT (:curr_timestamp) AS curr_date_time
                        ),
                        start_time_check AS
                        (
                            SELECT (rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago') AS start_date_time
                            FROM plc.sensor_data_transactional sdt
                            WHERE rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' > (:curr_timestamp)::date + INTERVAL '3 hours 45 minutes'
                            AND rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' <= (:curr_timestamp)::date + INTERVAL '24 hours'
                            AND status = 'Start'
                            ORDER BY rec_crtd_ts ASC
                            LIMIT 1
                        ),
                        stop_time_check AS
                        (
                            SELECT (rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago') AS stop_date_time
                            FROM plc.sensor_data_transactional sdt
                            WHERE rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' > (:curr_timestamp)::date + INTERVAL '3 hours 45 minutes' 
                            AND rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' <= (:curr_timestamp)::date + INTERVAL '24 hours' 
                            AND status = 'Stop'
                            ORDER BY rec_crtd_ts DESC
                            LIMIT 1
                        ),
                        pci_per_min AS
                        (
                            SELECT 
                            date_trunc('minute',rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago') AS rec_crtd_trunc, 
                            sum(piece_count_in::int) AS piece_count_in
                            FROM plc.sensor_data_analytics AS sda1
                            WHERE 
                            sda1.rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago'  > (SELECT start_date_time FROM start_time_check)
                            AND
                            sda1.rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago'  <= (SELECT start_date_time FROM start_time_check)::date + INTERVAL '24 hours'
                            GROUP BY date_trunc('minute',rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago')
                            ORDER BY rec_crtd_trunc desc 
                        ),
                        pci_fil AS
                        (
                            SELECT rec_crtd_trunc, piece_count_in, LAG(piece_count_in) OVER (ORDER BY rec_crtd_trunc) AS previous_piece_count_in 
                            FROM pci_per_min
                            ORDER BY rec_crtd_trunc DESC
                        ),
                        end_time_check AS
                        (
                            SELECT rec_crtd_trunc AS end_date_time
                            FROM pci_fil pf
                            WHERE pf.previous_piece_count_in - pf.piece_count_in >= 7
                            LIMIT 1
                        ),
                        avg_pc AS
                        (
                            SELECT
                            sum(piece_count_in::int) AS piece_count_in, sum(piece_count_out::int) AS piece_count_out, round((sum(piece_count_in::int)+sum(piece_count_out::int))/2,2) AS avg_piece_count,round(avg(speed::numeric),2) AS avg_speed
                            FROM plc.sensor_data_analytics AS sda1
                            WHERE 
                            sda1.rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago'  > (SELECT start_date_time FROM start_time_check)
                            AND
                            sda1.rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago'  <= (SELECT start_date_time FROM start_time_check)::date + INTERVAL '24 hours'
                        ),
                        movement_check AS 
                        (
                            SELECT rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' AS rec_crtd_ts, status
                            FROM plc.sensor_data_transactional sda 
                            WHERE rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' >= (SELECT start_date_time FROM start_time_check)
                            AND rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' < (SELECT start_date_time FROM start_time_check)::date + INTERVAL '24 hours'
                            ORDER BY rec_crtd_ts 
                        ),
                        movement_timeseries AS 
                        (
                            SELECT rec_crtd_ts, status, lead(rec_crtd_ts) OVER (ORDER BY rec_crtd_ts) AS next_entry, lead(status) OVER (ORDER BY rec_crtd_ts) AS next_status
                            FROM movement_check mc
                        ),
                        movement_dur AS
                        (
                            SELECT rec_crtd_ts AS start_time, next_entry AS stop_time, next_entry-rec_crtd_ts AS line_active_duration 
                            FROM movement_timeseries 
                            WHERE status <> next_status AND status = 'Start'
                        ),
                        line_mov_dur AS 
                        (
                            SELECT date_trunc('seconds',sum(md.line_active_duration)) AS line_active_dur
                            FROM movement_dur md
                        ),
                        non_productivity_raw AS
                        (
                            SELECT 
                            rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' AS start_time, 
                            rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' + INTERVAL '10 seconds' AS stop_time,
                            (rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago' + INTERVAL '10 seconds') - (rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago') AS duration,
                            piece_count_in::int AS piece_count_in
                            FROM plc.sensor_data_analytics AS sda1
                            WHERE 
                            sda1.rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago'  > (SELECT start_date_time FROM start_time_check)
                            AND
                            sda1.rec_crtd_ts AT time ZONE 'utc' AT time ZONE 'America/Chicago'  <= (SELECT stop_date_time FROM stop_time_check)
                            AND
                            piece_count_in::int < 2
                            ORDER BY rec_crtd_ts asc
                        ),
                        non_productivity AS
                        (
                            SELECT sum(duration) as non_productive_hours FROM non_productivity_raw
                        )
                        SELECT 
                        tc.curr_date_time::date AS curr_date,
                        date_trunc('second',stc.start_date_time):: time AS start_time,
                        date_trunc('second',sttc.stop_date_time):: time AS stop_time,
                        lmd.line_active_dur AS up_duration,
                        date_trunc('second',(sttc.stop_date_time:: time - stc.start_date_time:: time)) - lmd.line_active_dur AS down_duration,
                        etc.end_date_time:: time AS last_active_time,
                        np.non_productive_hours AS inactive_duration,
                        (date_trunc('second',(sttc.stop_date_time:: time - stc.start_date_time:: time)) - lmd.line_active_dur)+np.non_productive_hours AS non_productive_hours,
                        ap.avg_piece_count AS avg_piece_count, ap.avg_speed
                        FROM time_check tc, start_time_check stc, stop_time_check sttc, end_time_check etc, avg_pc ap,line_mov_dur lmd, non_productivity np"""
            curr_values = dict(curr_timestamp=curr_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            curr_query_res = run_query(curr_engine,curr_query,curr_values)
            print(curr_query_res)
            insert_values = dict()
            for row in curr_query_res:
                report_data_dict['date'].append(end_date)
                insert_values['report_date'] = str(end_date)
                report_data_dict['start_time'].append(str(row[1]))
                insert_values['start_time'] = str(str(row[1]))
                report_data_dict['stop_time'].append(str(row[2]))
                insert_values['stop_time'] = str(str(row[2]))
                report_data_dict['up_duration'].append(str(row[3]))
                insert_values['up_duration'] = str(str(row[3]))
                report_data_dict['down_duration'].append(str(row[4]))
                insert_values['down_duration'] = str(str(row[4]))
                report_data_dict['last_active_time'].append(str(row[5]))
                insert_values['last_active_time'] = str(str(row[5]))
                report_data_dict['inactive_duration'].append(str(row[6]))
                insert_values['inactive_duration'] = str(str(row[6]))
                report_data_dict['non_productive_hours'].append(str(row[7]))
                insert_values['non_productive_hours'] = str(str(row[7]))
                report_data_dict['avg_piece_count'].append(str(row[8]))
                insert_values['avg_piece_count'] = str(str(row[8]))
                report_data_dict['avg_speed'].append(str(row[9]))
                insert_values['avg_speed'] = str(str(row[9]))
            curr_query = f"""INSERT INTO plc.reports(report_date,start_time,stop_time,up_duration,down_duration,last_active_time,inactive_duration,non_productive_hours,avg_piece_count,avg_speed)
                            VALUES
                            (:report_date,:start_time,:stop_time,:up_duration,:down_duration,:last_active_time,:inactive_duration,:non_productive_hours,:avg_piece_count,:avg_speed)
                            RETURNING report_date"""
            if len(insert_values) > 0:
                curr_query_res = run_query(curr_engine,curr_query,insert_values)
    except Exception as e:
        print()
        print("Error Occured while trying to fecth report data")
        print()
        print(traceback.format_exc())
        print()
    finally:
        if curr_engine is not None:
            curr_engine.dispose()
    report_data_df = pd.DataFrame.from_dict(report_data_dict)
    report_data_df = report_data_df.sort_values(by=['date'],ascending=False).reset_index(drop=True).head(14)
    return report_data_df
    
def create_report():
    st.header('Production Line Reports', divider='rainbow')
    with st.container(border=True):
        st.markdown(
                    """
                        #### :orange[Important Points]
                        - :grey[The paint line report shows last 14 days data]
                        - :grey[The paint line report will update today's data only after 6 pm]
                        - :grey[There will be no data for holidays]
                            
                    """)
    with st.expander(label="",expanded=True):
        st.markdown(
                    """
                        ##### :orange[Synonyms:]
                        - :grey[date: Represents the date of the record]
                        - :grey[start_time: First time the sensor marked the production line as running after 3:45 am]
                        - :grey[stop_time: Last time the sensor marked the production line as stopped after 3:45 am]
                        - :grey[up_duration: Duration for which the production line was in running state]
                        - :grey[last_active_time: Last time since there were incoming pieces in the production line]
                        - :grey[inactive_duration: Total time duration when the production line as running but there were no incoming pieces in the production line]
                        - :grey[non_productive_hours: Total time when either the paint line is idle or there were no incoming pieces in the production line]
                        - :grey[avg_piece_count: Average of piece_count_in and piece_count_out]
                        - :grey[avg_speed: Average speed]
                            
                    """)
    with st.spinner("Downloading data"):
        report_df = fetch_report_data()  
    st.toast('Data Downloaded Successfully!', icon='✅')
    st.snow()
    with st.container(border=True):
        st.dataframe(report_df,hide_index=True,width=1300)
    

create_report()


