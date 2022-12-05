
import datetime
import logging
import matplotlib.pyplot as plt
import pandas as pd
import plotnine as gg
from prophet import Prophet
from prophet.plot import add_changepoints_to_plot
from sklearn.model_selection import train_test_split
import seaborn as sns
import time

from DBConn import DBConn
from AppleHealthDB import AppleHealthDB
from raw_data_process import create_database, add_sleep_data

# class AppleHealthDB(DBConn):
#     def __init__(self, db_name: str):
#         super().__init__(db_name=db_name, filepath="./../data/")

# Logging  
# Create formatter and the two handlers
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - %(name)s')
f_handler = logging.FileHandler('log.log')
s_handler = logging.StreamHandler()

# Set the levels and formats of the handlers
f_handler.setLevel(logging.DEBUG)
s_handler.setLevel(logging.INFO)
f_handler.setFormatter(log_format)
s_handler.setFormatter(log_format)

# Get the logger
logger = logging.getLogger(__name__)   
logger.setLevel(logging.DEBUG)
logger.addHandler(f_handler)
logger.addHandler(s_handler)


if __name__ == '__main__':


    # Create the database from the XML file
    db: AppleHealthDB = create_database('export-2022-11-27', 'AutoSleep-19991106-to-20221203.csv')
    # db: AppleHealthDB = create_database('partial', 'AutoSleep-19991106-to-20221203.csv')
    
    
    heartrate = "HKQuantityTypeIdentifierHeartRate"
    resting_heartrate = "HKQuantityTypeIdentifierRestingHeartRate"
    walking_heartrate = "HKQuantityTypeIdentifierWalkingHeartRateAverage"
    resp_rate = "HKQuantityTypeIdentifierRespiratoryRate"
    flights = "HKQuantityTypeIdentifierFlightsClimbed"
    steps = "HKQuantityTypeIdentifierStepCount"
    cycling_distance = "HKQuantityTypeIdentifierDistanceCycling"
    exercise_time = "HKQuantityTypeIdentifierAppleExerciseTime"

    # ['HKQuantityTypeIdentifierHeight', 'HKQuantityTypeIdentifierBodyMass', 
    # 'HKQuantityTypeIdentifierHeartRate', 'HKQuantityTypeIdentifierBloodPressureSystolic', 
    # 'HKQuantityTypeIdentifierBloodPressureDiastolic', 'HKQuantityTypeIdentifierRespiratoryRate', 
    # 'HKQuantityTypeIdentifierStepCount', 'HKQuantityTypeIdentifierDistanceWalkingRunning', 
    # 'HKQuantityTypeIdentifierBasalEnergyBurned', 'HKQuantityTypeIdentifierActiveEnergyBurned', 
    # 'HKQuantityTypeIdentifierFlightsClimbed', 'HKQuantityTypeIdentifierNikeFuel', 
    # 'HKQuantityTypeIdentifierAppleExerciseTime', 'HKQuantityTypeIdentifierDistanceCycling', 
    # 'HKQuantityTypeIdentifierRestingHeartRate', 'HKQuantityTypeIdentifierVO2Max', 
    # 'HKQuantityTypeIdentifierWalkingHeartRateAverage', 'HKQuantityTypeIdentifierEnvironmentalAudioExposure', 
    # 'HKQuantityTypeIdentifierHeadphoneAudioExposure', 'HKQuantityTypeIdentifierWalkingDoubleSupportPercentage', 
    # 'HKQuantityTypeIdentifierSixMinuteWalkTestDistance', 'HKQuantityTypeIdentifierAppleStandTime', 
    # 'HKQuantityTypeIdentifierWalkingSpeed', 'HKQuantityTypeIdentifierWalkingStepLength', 
    # 'HKQuantityTypeIdentifierWalkingAsymmetryPercentage', 'HKDataTypeSleepDurationGoal', 
    # 'HKQuantityTypeIdentifierAppleWalkingSteadiness', 'HKQuantityTypeIdentifierRunningSpeed', 
    # 'HKCategoryTypeIdentifierSleepAnalysis', 'HKCategoryTypeIdentifierAppleStandHour', 
    # 'HKCategoryTypeIdentifierHighHeartRateEvent', 'HKCategoryTypeIdentifierLowHeartRateEvent', 
    # 'HKCategoryTypeIdentifierAudioExposureEvent', 'HKCategoryTypeIdentifierRapidPoundingOrFlutteringHeartbeat', 
    # 'HKCategoryTypeIdentifierSkippedHeartbeat', 'HKCategoryTypeIdentifierDizziness', 
    # 'HKCategoryTypeIdentifierHandwashingEvent', 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN']

    # df = pd.DataFrame(db.record_by_date_range(flights, '2022-08-10', '2022-08-20').fetchall(), columns=['id', 'record_type', 'unit', 'creation_date', 'start_date', 'end_date', 'value']).set_index('id')
    # df = pd.DataFrame(db.sum_by_date_range(steps, start_date='2018-10-01').fetchall(), columns=['date', 'value']).set_index('date')
    # df['value'] = df['value'].apply(lambda x: 30000 if x > 50000 else x)
    # # print(df)

    # g = gg.ggplot(df, gg.aes(x=df.index, y='value')) + gg.geom_col(fill='#ff4f00') + gg.scale_x_datetime(date_breaks="5 months", expand=(0,0))
    # f = gg.ggplot(df, gg.aes(x='value')) + gg.geom_histogram(binwidth=1000, fill='#ff4f00')
    # print(f)

    sql_query = """
    SELECT SUBSTR(d.starting_date, 0, 5) as year,
           SUBSTR(d.starting_date, 6, 5) as day,
           SUM(record_value) as value
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
      AND d.starting_date BETWEEN DATE(?) AND DATE(?)
    GROUP BY 1, 2
    """

    # df2 = pd.DataFrame(db.execute_query(sql_query, (flights, '2018-10-01', db.last_date)).fetchall(), columns=['year', 'day', 'value'])
    # f = (gg.ggplot(df2, gg.aes(x='value')) 
    #    + gg.geom_histogram(binwidth=1, fill='#ff4f00', colour='black')
    #    + gg.facet_grid('year ~ .'))
    # print(f)

    # Get the summary steps for a day
    # Get the quality and deep sleep for a day
    # join on date

    # Daily: resting heart rate, walking heartrate average, 
    # Sum:   step count, flights, distance cycling, apple exercise time, 
    # Avg:   resp rate, 
    # Min/Max(avg?): walking speed, 

    sql_query2 = """
    SELECT COALESCE(one.date, three.date), two.bedtime, two.waketime, one.steps, one.flights, one.cycling_distance, one.exercise_time, one.resp_rate_avg, two.efficiency, two.quality, two.deep, two.awake, two.fell_asleep_in, two.in_bed, three.resting_heartrate, four.walking_heartrate
    FROM
    (SELECT SUBSTR(d.starting_date, 0, 11) as date,
            SUM(CASE r.record_type WHEN ? THEN d.record_value ELSE 0 END) as steps,
            SUM(CASE r.record_type WHEN ? THEN d.record_value ELSE 0 END) as flights,
            SUM(CASE r.record_type WHEN ? THEN d.record_value ELSE 0 END) as cycling_distance,
            SUM(CASE r.record_type WHEN ? THEN d.record_value ELSE 0 END) as exercise_time,
            AVG(CASE r.record_type WHEN ? THEN d.record_value ELSE NULL END) as resp_rate_avg
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ? OR r.record_type = ? OR r.record_type = ? OR r.record_type = ? OR r.record_type = ?
      AND d.starting_date BETWEEN DATE(?) AND DATE(?)
    GROUP BY 1) one
    JOIN
    (SELECT date,
            bedtime,
            waketime,
            efficiency,
            quality,
            deep,
            awake,
            fell_asleep_in,
            in_bed
    FROM Sleep
    WHERE date BETWEEN DATE(?) and DATE(?)) two
    ON one.date = two.date
    RIGHT JOIN
    (SELECT DISTINCT SUBSTR(d.starting_date, 0, 11) as date,
            record_value AS resting_heartrate
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
    ) three
    on one.date = three.date
    JOIN
    (SELECT SUBSTR(d.starting_date, 0, 11) as date,
            record_value AS walking_heartrate
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
    ) four
    on three.date = four.date
    """

    sql_query3 = """
    SELECT SUBSTR(d.starting_date, 0, 11) as date,
           record_value AS resting_heartrate
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
    """

    sql_query4 = """
    SELECT  SUBSTR(d.starting_date, 0, 11) as date,
            AVG(CASE r.record_type WHEN ? THEN d.record_value ELSE NULL END) as min_heart_rate
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
      AND d.record_value < 140
    GROUP BY 1"""

    df3 = pd.DataFrame(db.execute_query(sql_query2, (*([steps, flights, cycling_distance, exercise_time, resp_rate] * 2), '2020-03-09', '2022-12-01', '2020-03-09', '2022-12-01', resting_heartrate, walking_heartrate)), 
                       columns=['date', 'bedtime', 'waketime', 'steps', 'flights', 'cycling_distance', 'exercise_time', 'efficiency', 'resp_rate', 'quality', 'deep', 'awake', 'fell_asleep_in', 'in_bed', 'resting_heartrate', 'walking_heartrate'])
    # df3 = pd.DataFrame(db.execute_query(sql_query3, (resting_heartrate,)), columns=['date', 'resting_heartrate'])
    # df3 = pd.DataFrame(db.execute_query(sql_query4, (heartrate, heartrate)), columns=['date', 'min_heartrate'])
    print(df3)

    cutoff_date = '2022-01-01'

    # df_train = df3.loc[df3['date'] < cutoff_date].copy()
    # df_test = df3.loc[df3['date'] >= cutoff_date].copy()
    

    # h = gg.ggplot(df3, gg.aes(x='bedtime', y='cycling_distance')) + gg.geom_point()  + gg.scale_x_datetime(date_breaks='6 months')
    # print(h)

    # k = sns.heatmap(df3.corr(), cmap='PiYG')
    # plt.show()
    # k.get_figure().savefig('./../plots/corr.png', dpi=200)

    
    proph = Prophet(changepoint_range=0.9)
    proph.add_seasonality(name='yearly', period=365, fourier_order=6).fit(df3[['date', 'resting_heartrate']].rename(columns={'date': 'ds', 'resting_heartrate': 'y'}))

    a = proph.predict(proph.make_future_dataframe(periods=365))

    fig = proph.plot(a)
    c = add_changepoints_to_plot(fig.gca(), proph, a)
    # proph.plot_components(a)
    # plt.xlim([datetime.date.fromisoformat('2022-06-01'), datetime.date.fromisoformat('2023-05-01')])
    plt.show()

    # g = gg.ggplot(a[['ds', 'yhat']], gg.aes(x='ds', y='yhat')) + gg.geom_point()
    # print(g)
