
import pandas as pd
import numpy as np

df=pd.read_csv(r"D:\Project1\progress.csv")

print("===== ORIGINAL DATA =====")
print(df)

df=df.dropna(subset=['student_name'])

df['enrollment_date']=pd.to_datetime(df['enrollment_date'],errors='coerce')

df['completion']=df['completion'].fillna(0)

df['completion']=np.clip(df['completion'],0,100)

print("\n===== CLEANED DATA =====")
print(df)

average_progress=np.mean(df['completion'])

print("\n===== OVERALL AVERAGE PROGRESS =====")
print(f"Average Completion Percentage: {average_progress:.2f}%")

course_summary=df.groupby('course_name')['completion'].mean()

print("\n===== COURSE-WISE AVERAGE COMPLETION =====")
print(course_summary)

low_progress_students=df[df['completion']<50]

print("\n===== STUDENTS BELOW 50% PROGRESS =====")
print(low_progress_students)

popular_courses=df.groupby('course_name')['student_id'].count().sort_values(ascending=False)

print("\n===== POPULAR COURSES =====")
print(popular_courses)

df.to_csv(r"D:\Project1\cleaned_progress.csv",index=False)

print("\ncleaned_progress.csv exported successfully")
