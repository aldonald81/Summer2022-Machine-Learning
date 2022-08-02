# Modeling Appointment No-Shows
## Model Goal
The goal of this model is to predict the probability of a given filled appointment slot for internal medicine between the hours of 7am and 5pm being a ‘No-Show’ based on appointment characteristics such as time, day, patient demographics, etc. This goal changed over time as I learned more about the data and how I could make the data more predictable. One of the key changes was pivoting from predicting whether a slot would go unfilled to a no-show. This significantly narrowed the focus of the model since an unfilled slot could have been 'Open,' 'Cancelled,' or a 'No-Show.' This change improved the model's ability to fit to the data significantly

## Data Features and Pre-Processing
The features for the model include:
- DAYOFWEEK
- MONTH
- APPOINTMENTSTARTTIME_HOUR
- SPECIALTY
- AGE_CATEGORY
- SEX
- LANGUAGE
- ETHNICITY
- RACE
- MARITALSTATUS
- SMSOPTIN
- previous_noshow  
- DATE_DIFF_CATEGORY  

These features are a mixture of slot and patient level characteristics. Some features that I engineered that significantly improved the model were previous_noshow, which is a count of the previous no-shows for the specific patient filling a slot, and DATE_DIFF_CATEGORY, which represents the amount of time between appointment scheduled date and appointment date. All numerical features were converted to categorical with the help of `smbinning` library in R.

The dependent variable is slot_noShow and flags an appointment slot that has already happened and was a no-show as a 1

## Modeling
Both the final_classweights5000epochs_model model and the SMOTE_sampling_model model use `LogisticRegression()` from Python's `sklearn.linear_model` module to fit the model to the training data. For training, all appointment slots from 2021 that were filled were used as examples. For testing, all appointment slots from the first half of 2022 that were filled were used as examples. The final_classweights5000epochs_model model uses a `class_weight='balanced'` parameter that adjusts weights inversely proportional to class frequencies in the input data. For the SMOTE_sampling_model, `imblearn.over_sampling` and `imblearn.under_sampling` were used to implement over and under sampling to even out class distribution. Both the `class_weight` parameter and the sampling methods were attempts to weight classes evenly while training the model even though roughly 3% of the examples were marked as a no-show.


## Results
### final_classweights5000epochs_model
- Threshold set to .2 (prediction above .2 predicts no-show)
- Accuracy: 0.85
- ROC Area Under Curve: 0.88
- Recall: .91
- Precision: .22
- F1 score: 0.35
- True Positives: 4,766
- False Positives: 16,971
- True Negatives: 95,124
- False Negatives: 453


### SMOTE_sampling_model
- Threshold set to .3 (prediction above .3 predicts no-show)
- Accuracy: 0.83
- ROC Area Under Curve: 0.91
- Recall: .99
- Precision: .21
- F1 score: 0.35
- True Positives: 5,194
- False Positives: 19,543
- True Negatives: 92,552
- False Negatives: 25

## Analysis of SMOTE_sampling_model
![image](https://user-images.githubusercontent.com/109293130/180277816-bc8357d0-8b70-4dcb-977c-7e6e826260b7.png)

![image](https://user-images.githubusercontent.com/109293130/180277938-c384ec77-2caa-4436-b2d7-3e78ae186920.png)

*** scaled using the avg ratio (trailing 4 months) of total predicted no-shows/day : actual no-shows/day ***
