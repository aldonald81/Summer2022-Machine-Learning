#' No-Show Model
#'
#' The get_noshow_data() function queries the Snowflake appointments table
#' and returns a data frame to use for model predictions
#'
#' @param connection Pass the snowflake connection
#' @param startdate Pass the start date for appointment data based on appointment
#' date
#' @param enddate Pass the end date for appointment data based on appointment
#' date
#' @return A data frame of appointment slots and info
#' @keywords No Show Data

get_noshow_data <- function(connection, start_date, end_date){
  library(stringr)
  library(dplyr)

  a <- str_glue("
with df as (SELECT
a.parentappointmentid
,a.appointmentid
,a.appointmentdate
,a.appointmentstarttime
,a.appointmentcreateddatetime
,(CASE WHEN D.DEPARTMENTNAME LIKE 'Tryon Med %' Then (REPLACE(D.DEPARTMENTNAME, 'Tryon Med - ', ''))
    when D.DEPARTMENTNAME LIKE '%COVID%' then 'Tryon Med COVID Clinic - Pineville'
    when D.DEPARTMENTNAME = 'Tryon Med-Asheville' Then 'Asheville'
    when D.DEPARTMENTNAME LIKE '%SATELLITE%' then 'Tryon Med Satellite-Uptown 7th' END)
    AS DEPARTMENTNAME
,pt.patientid
,pt.enterpriseid
,DATEDIFF(year, pt.dob, a.appointmentdate) as age
,pt.sex
,LEFT(pt.zip, 5) as zip
,p.schedulingname
,concat(p.providerlastname, ', ', p.providerfirstname) as physician_name
,p.specialty
,p.providertypecategory
,p.providertype
,ap.appointmenttypename
,ap.appointmenttypeclass
,a.appointmentstatus
,A.APPOINTMENTCANCELLEDDATETIME AS APTCANCELLEDDATE
,DATEDIFF(day, A.APPOINTMENTCANCELLEDDATETIME, A.APPOINTMENTDATE) AS TIMETOCANCEL
,a.appointmentcancelreason
,a.frozenyn
,a.appointmentfrozenreason
,(CASE
    WHEN (DATEDIFF(day, A.APPOINTMENTCANCELLEDDATETIME, A.APPOINTMENTDATE) = 0
            AND
            (A.APPOINTMENTCANCELREASON <> 'PATIENT NO SHOW'
            OR UPPER(A.APPOINTMENTCANCELREASON) <> 'NO SHOW NO CALL')
            )
        THEN 'Same Day Cancellation'
    WHEN DATEDIFF(day, A.APPOINTMENTCANCELLEDDATETIME, A.APPOINTMENTDATE) > 0
        THEN 'Cancelled Before Scheduled Appointment'
    WHEN (DATEDIFF(day, A.APPOINTMENTCANCELLEDDATETIME, A.APPOINTMENTDATE) < 0
            OR
            UPPER(A.APPOINTMENTCANCELREASON) IN ('PATIENT NO SHOW', 'NO SHOW NO CALL'))
        THEN 'No Call, No Show'
    WHEN APPOINTMENTSTATUS = '4 - Charge Entered'
  THEN 'Attended'
        ELSE 'Open Slot'
    END) AS APPOINTMENT_STATUS_CAT

-- SELECTING other patient level data to add to model
, pt.LANGUAGE
, pt.CDCETHNICITYCODE
, pt.ETHNICITY
, pt.CDCRACECODE
, pt.RACE
, pt.ZIP AS PATIENT_ZIP
, pt.DONOTCALLYN
, pt.MARITALSTATUS
, pt.REGISTRATIONDATE
, pt.PROVIDERGROUPID
, pt.UNCONFIRMEDFAMILYSIZE
, pt.UNCONFIRMEDYEARLYINCOME
, pt.SMSOPTINDATE
, d.LONGITUDE AS DEPARTMENT_LONGITUDE
, d.LATITUDE AS DEPARTMENT_LATITUDE

from APPOINTMENT a
join APPOINTMENTTYPE ap on a.appointmenttypeid = ap.appointmenttypeid
join DEPARTMENT d on a.departmentid = d.departmentid
join PROVIDER p on a.schedulingproviderid = p.providerid
left join PATIENT pt on a.patientid = pT.patientid
where year(a.appointmentdate) >= year(current_date()) - 3
AND a.appointmentdate <= '{end_date}'
AND a.appointmentdate >= '{start_date}'
AND (UPPER(DEPARTMENTNAME) LIKE 'TRYON MED%' OR UPPER(DEPARTMENTNAME) LIKE 'TMP%')
and p.entitytype = 'Person'
and (pt.patientstatus not in ('d', 'i') or pt.patientstatus is null)
and upper(p.schedulingname) not like '%TEST%'
and (pt.testpatientyn = 'N' or pt.testpatientyn is null)
AND (FROZENYN = 'N' OR FROZENYN IS NULL)
AND DAYOFWEEK(a.APPOINTMENTDATE) <> 0 AND DAYOFWEEK(a.APPOINTMENTDATE)<>6 -- FILTER OUT WEEKENDS
AND (TIME(a.APPOINTMENTSTARTTIME) >= '07:00:00' AND TIME(a.APPOINTMENTSTARTTIME) < '17:00:00') --only slots between 7am and 5pm

),
df1 as (
select distinct
df.*
,concat(appointmentdate, schedulingname, appointmentstarttime) as SLOT_ID
,Row_number() over (partition by SLOT_ID order by appointmentcreateddatetime DESC) as SLOT_IDX -- Adding id to each appointment created within a time slot for specific doctor (Descending by date - most recent = 1)
from df
WHERE DEPARTMENTNAME != 'Randolph GI & IM' AND DEPARTMENTNAME != 'Virtual Connect'
--where schedulingname like '%Alexander%'
--and appointmentdate = '2021-03-08'
--and appointmentstarttime = '08:30 AM'
),
df2 as (
SELECT
  df1.*,
  LEAD(APPOINTMENTSTATUS,1, APPOINTMENTSTATUS)  -- looking to next row (back in time) to see the status of next most recent appointment
  OVER (PARTITION BY [APPOINTMENTSTARTTIME],[SCHEDULINGNAME], [APPOINTMENTDATE]  -- Grouping by slot by doctor
  ORDER BY SLOT_IDX DESC)  AS nextStatusType -- Order by latest to most recent appointments
  FROM df1
),
df3 as (
SELECT df2.*,
CASE
  WHEN APPOINTMENTSTATUS = 'x - Cancelled' AND SLOT_IDX <=2 AND nextStatusType = 'o - Open Slot' THEN 0 -- Case when the next appointment for slot is Open so must not have been filled -> therefore flag cancelled one so we can record that one
  ELSE SLOT_IDX -- Otherwise keep the same
END AS importance -- Gets flagged as 0 if most recent is listed as open but should be cancelled (flags cancelled appointment)
FROM df2
),
--find most recent ACTUAL appointment for each slot
df4 as (
SELECT
  APPOINTMENTDATE,
  APPOINTMENTSTARTTIME,
  SCHEDULINGNAME,
  MIN(IMPORTANCE) AS importance_id -- Same as SLOT_IDX unless it was flagged as 0
  FROM df3
  GROUP BY APPOINTMENTDATE,APPOINTMENTSTARTTIME, SCHEDULINGNAME
),
df5 AS (
  SELECT df3.*, DAYOFMONTH(df4.APPOINTMENTDATE) AS DAYOFMONTH, DAYNAME(df4.APPOINTMENTDATE) AS DAYOFWEEK, DAYOFYEAR(df4.APPOINTMENTDATE) AS DAYOFYEAR, MONTHNAME(df4.APPOINTMENTDATE) AS MONTH, YEAR(df4.APPOINTMENTDATE) AS YEAR, TIME(df4.APPOINTMENTSTARTTIME) AS APPOINTMENTSTARTTIME_military, HOUR(TIME(df4.APPOINTMENTSTARTTIME)) AS APPOINTMENTSTARTTIME_hour FROM
df4 LEFT JOIN df3
ON df3.APPOINTMENTDATE = df4.APPOINTMENTDATE
    AND df3.APPOINTMENTSTARTTIME = df4.APPOINTMENTSTARTTIME
    AND df3.SCHEDULINGNAME = df4.SCHEDULINGNAME
    AND df3.IMPORTANCE = df4.importance_id
) ,
-- Getting no show count history per patient
df6 as (
SELECT PATIENTID, IFNULL(COUNT(PATIENTID),0) AS NOSHOW_COUNT FROM df5
WHERE APPOINTMENT_STATUS_CAT = 'No Call, No Show'
GROUP BY PATIENTID
)  ,
df7 as (
SELECT df5.PATIENTID,
  YEAR, DAYOFYEAR, DAYOFMONTH, DAYOFWEEK, MONTH,
  APPOINTMENTSTARTTIME_HOUR, SPECIALTY,
  AGE, SEX,
  CASE WHEN LANGUAGE = 'English' THEN 'ENGLISH' ELSE 'Other' END AS LANGUAGE

  , CASE
        WHEN CDCRACECODE = '1002-5' THEN 'American Indian or Alaska Native'
        WHEN CDCRACECODE = '2028-9' THEN 'Asian'
        WHEN CDCRACECODE = '2054-5' THEN 'Black or African American'
        WHEN CDCRACECODE = '2076-8' THEN 'Native Hawaiian or Other Pacific Islander'
        -- WHEN CDCRACECODE = '2131-1' THEN 'Other Race'
        WHEN CDCRACECODE = '2106-3' THEN 'White'
        WHEN RACE = 'Patient Declined' AND CDCRACECODE IS NULL THEN 'Patient Declined'
        ELSE 'Other'
    END AS RACE
  , CASE
        WHEN ETHNICITY = 'Not Hispanic or Latino' THEN 'Non-Hispanic or Latin American'
        WHEN ETHNICITY = 'Patient Declined' THEN 'Patient Declined'
        ELSE 'Hispanic or Latin American'
    END AS ETHNICITY
  --, CASE
   -- WHEN RACE = 'Asian' THEN 'Asian'
   -- WHEN RACE = 'White' OR RACE = 'European' THEN 'Caucasian'
   -- WHEN RACE = 'African American' OR RACE = 'Black' OR 'Black or African American' THEN 'African American'
   -- WHEN RACE = 'Hispanic' THEN
   -- WHEN RACE = 'Patient Declined' THEN 'Patient Declined'


  ,PATIENT_ZIP
  ,DEPARTMENT_LONGITUDE
  ,DEPARTMENT_LATITUDE
--  ,DONOTCALLYN --Only 66 rows with 'Y' so don't think its good indicator
  ,CASE
    WHEN MARITALSTATUS = 'DIVORCED' OR MARITALSTATUS = 'SEPARATED' OR MARITALSTATUS = 'SINGLE' OR MARITALSTATUS = 'WIDOWED' THEN 'Single'
    WHEN MARITALSTATUS = 'MARRIED' OR MARITALSTATUS = 'PARTNER' THEN 'PARTNER'
    ELSE 'OTHER'
  END AS MARITALSTATUS

--  ,REGISTRATIONDATE  -- POSSIBLY ADD LATER
--  ,PROVIDERGROUPID
--  ,UNCONFIRMEDFAMILYSIZE
--  ,UNCONFIRMEDYEARLYINCOME
  ,CASE WHEN SMSOPTINDATE IS NOT NULL THEN 'Y' ELSE 'N' END AS SMSOPTIN
  ,NOSHOW_COUNT,
  APPOINTMENTCREATEDDATETIME, APPOINTMENTDATE, APPOINTMENTSTATUS, APPOINTMENT_STATUS_CAT,
  DATEDIFF(day, APPOINTMENTCREATEDDATETIME, APPOINTMENTDATE) AS DATE_DIFF FROM
df5 LEFT JOIN df6
ON df5.PATIENTID = df6.PATIENTID
)
SELECT *,
    CASE
        WHEN DATE_DIFF <= 3 THEN '0-3'
        WHEN DATE_DIFF <= 130 AND DATE_DIFF > 3 THEN '4-130'
        WHEN DATE_DIFF <= 649 AND DATE_DIFF > 130 THEN '131-649'
        WHEN DATE_DIFF > 649 THEN '>649'
     END AS DATE_DIFF_CATEGORY,
     CASE
        WHEN AGE <= 26 THEN '<=26'
        WHEN AGE <= 34 AND AGE > 26 THEN '27-34'
        WHEN AGE <= 39 AND AGE > 34 THEN '35-39'
        WHEN AGE <= 53 AND AGE > 39 THEN '40-53'
        WHEN AGE <= 64 AND AGE > 53 THEN '54-64'
        WHEN AGE <= 82 AND AGE > 64 THEN '65-82'
        WHEN  AGE > 82 THEN '>82'
      END AS AGE_CATEGORY,
     CASE  --ADDING A KNOT in age continuous variable
        WHEN AGE >= 75 THEN AGE - 75
        ELSE 0
     END AS AGE_AFTER75,
     CASE
        WHEN AGE < 75 THEN AGE
        ELSE 75
     END AS AGE_BEFORE75

FROM df7

")

  a <- str_replace(a, "glue", "")

  result <- DBI::dbGetQuery(connection, a)
  result$NOSHOW_COUNT[is.na(result$NOSHOW_COUNT)] <- 0
  # Want the slot to be filled since it will have patient data
  result <- result %>%
    filter(APPOINTMENTSTATUS == "4 - Charge Entered" |
             APPOINTMENTSTATUS == "x - Cancelled") %>%
    dplyr::filter(SPECIALTY == "Internal Medicine") %>% #EDIT
    dplyr::mutate(slot_noShow = case_when(
      APPOINTMENTSTATUS == 'x - Cancelled'  &
        APPOINTMENT_STATUS_CAT == 'No Call, No Show' ~ 1,
      TRUE ~ 0)) %>%
    # mutate(previous_noshow = case_when( #EDIT
    #       NOSHOW_COUNT == 0 ~ 'None',
    #       NOSHOW_COUNT >0 & NOSHOW_COUNT <=4 ~ 'Some',
    #       NOSHOW_COUNT > 4 & NOSHOW_COUNT <=10 ~ 'Alot',
    #       TRUE ~ 'Aton')) %>%
    dplyr::mutate(previous_noshow = case_when( #EDIT
      NOSHOW_COUNT == 0 ~ '0',
      NOSHOW_COUNT == 1 ~ '1',
      NOSHOW_COUNT == 2 ~ '2',
      TRUE ~ '>2')) %>%
    dplyr::mutate(time_category = case_when( #EDIT
      APPOINTMENTSTARTTIME_HOUR <= 9 & APPOINTMENTSTARTTIME_HOUR >=14 ~ 'Ends of Day',
      TRUE ~ 'Middle of day')) %>%
    #dplyr::select( DAYOFMONTH, DAYOFWEEK, MONTH, APPOINTMENTSTARTTIME_HOUR, SPECIALTY, AGE, SEX, NOSHOW_COUNT, DATE_DIFF, slot_noShow)  # remove APPOINTMENTTYPECLASS, for now since it contains na # DEPARTMENTNAME, looks like one isnt' in test rn
    #dplyr::select(YEAR, DAYOFWEEK, MONTH, APPOINTMENTSTARTTIME_HOUR, SPECIALTY, AGE_CATEGORY, SEX, NOSHOW_COUNT, DATE_DIFF_CATEGORY, slot_noShow)
    #dplyr::select(YEAR, DAYOFWEEK, MONTH, APPOINTMENTSTARTTIME_HOUR, SPECIALTY, AGE_CATEGORY, SEX, LANGUAGE, ETHNICITY, RACE,  MARITALSTATUS, SMSOPTIN,  NOSHOW_COUNT, DATE_DIFF_CATEGORY, slot_noShow) #ADDRESS, DEPARTMENTADDRESS,
    # YEAR, DAYOFYEAR --- REMOVED FOR NOW

    #Going off this previous study
    dplyr::select(DAYOFWEEK, MONTH, time_category, SPECIALTY, AGE_CATEGORY,  SEX, LANGUAGE, ETHNICITY, RACE,  MARITALSTATUS, SMSOPTIN,  previous_noshow, DATE_DIFF_CATEGORY, slot_noShow)

  return(result)
}


#' Model prediction
#'
#' The predict_noshow() function takes in a data frame of appointments and
#' returns a prediction for each appointment slot.
#'
#' @param appointment_data Pass the result of get_noshow_data() as a parameter
#' @return A data frame with a probability of no-show for each appointment slot
#' @keywords predict no show
#' @export
#' @examples
#' q <- "SELECT TOP 100 * FROM dbo.C1Y"
#' df <- aws_fetch(q)
#'
predict_noshow <- function(df){
  # Need to get path to Tryon package in order to access stored data and model!
  formerPath <- getwd()
  packagePath <- .libPaths()
  packagePath <- paste(packagePath, "/Tryon", sep="")
  setwd(packagePath)

  #print(packagePath)

  save(df, file='./data/df.RData')

  library(reticulate)
  py_run_string("
# Get data that we trained on
import pyreadr
train = pyreadr.read_r('./data/train.RData')['a']

# Get data that we are predicting on
df = pyreadr.read_r('./data/df.RData')['df']

import pickle
# Load trained model
#loaded_model = pickle.load(open('C:/Users/AlexanderDonald/OneDrive - Tryon Direct/Desktop/GitHub/Tryon/models/test', 'rb'))
loaded_model = pickle.load(open('./models/SMOTE_sampling_model', 'rb'))

# Prepare data for model
import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.metrics import f1_score, accuracy_score, confusion_matrix
from sklearn.metrics import precision_recall_curve, roc_auc_score, precision_score, recall_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import make_column_transformer
from sklearn.compose import ColumnTransformer


# Drop rows with NAs
df = df.dropna()

x_test = df.loc[:, df.columns != 'slot_noShow']
x_train = train.loc[:, train.columns != 'slot_noShow']

y_test = df.loc[:, df.columns == 'slot_noShow']

#Scikit learn one hot encoder
#https://towardsdatascience.com/guide-to-encoding-categorical-features-using-scikit-learn-for-machine-learning-5048997a5c79
# https://www.dataschool.io/encoding-categorical-features-in-python/ -- Make pipelines
ohe = OneHotEncoder(sparse=False)
column_transform = make_column_transformer((ohe, ['MONTH', 'SPECIALTY', 'SEX', 'DAYOFWEEK',  'DATE_DIFF_CATEGORY', 'AGE_CATEGORY', 'LANGUAGE', 'ETHNICITY', 'RACE',  'MARITALSTATUS', 'SMSOPTIN', 'previous_noshow', 'time_category']), remainder='passthrough') # passes over the numeric column

encoder = column_transform.fit(x_train)

x_test = encoder.transform(x_test)

preds = loaded_model.predict_proba(x_test)[:,1]

# Using threshold of .3
bestThresh = .3
thresh = bestThresh
predsCat = []
for prob in preds: # Use numpy vector condition instead for faster running
    if prob >= thresh:
        predsCat.append(1)
    else:
      predsCat.append(0)


")

  preds <- py$preds
  rows <- py$df
  rows$Probability <- preds
  predsCat <- py$predsCat
  rows$'Prediction (thresh=.3)' <- predsCat

  setwd(formerPath)

  return(rows)

}



