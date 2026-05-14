use online_course_tracker

db.createCollection("feedback")

db.feedback.insertMany([
{student_id:1,course_id:1,student_name:"Rahul Sharma",course_name:"Python Basics",rating:5,review:"Excellent Python course with practical examples.",feedback_date:new Date("2026-05-10")},
{student_id:2,course_id:2,student_name:"Priya Reddy",course_name:"Data Engineering",rating:4,review:"Good Data Engineering content but slightly advanced.",feedback_date:new Date("2026-05-11")},
{student_id:3,course_id:3,student_name:"Arjun Kumar",course_name:"SQL Mastery",rating:3,review:"SQL course was useful but needed more exercises.",feedback_date:new Date("2026-05-12")},
{student_id:4,course_id:1,student_name:"Sneha Patel",course_name:"Python Basics",rating:5,review:"Loved the course structure and hands-on sessions.",feedback_date:new Date("2026-05-13")},
{student_id:5,course_id:2,student_name:"Aravind Rao",course_name:"Data Engineering",rating:4,review:"Very informative and useful for beginners.",feedback_date:new Date("2026-05-14")}
])

db.feedback.createIndex({student_id:1})

db.feedback.createIndex({course_id:1})

db.feedback.find()

db.feedback.find({student_id:1})

db.feedback.find({course_id:2})

db.feedback.updateOne({student_id:2},{$set:{rating:5,review:"Updated: Excellent course after completing more modules."}})

db.feedback.deleteOne({student_id:3})

db.feedback.aggregate([
{$group:{_id:"$course_name",average_rating:{$avg:"$rating"},total_reviews:{$sum:1}}}
])
